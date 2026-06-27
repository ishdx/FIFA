from fastapi import FastAPI, HTTPException, Depends, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pg8000.native, json, os, secrets, asyncio
from datetime import datetime

# ── SSE broadcaster ───────────────────────────────────────
class EventBroadcaster:
    def __init__(self):
        self._queues = []
    def subscribe(self):
        q = asyncio.Queue(maxsize=10)
        self._queues.append(q)
        return q
    def unsubscribe(self, q):
        if q in self._queues:
            self._queues.remove(q)
    async def broadcast(self, event: dict):
        dead = []
        for q in self._queues:
            try: q.put_nowait(event)
            except asyncio.QueueFull: dead.append(q)
        for q in dead:
            if q in self._queues: self._queues.remove(q)

broadcaster = EventBroadcaster()

app = FastAPI(title="FIFA WC2026 Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_USER   = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS   = os.environ.get("ADMIN_PASS", "smi2026")
security     = HTTPBasic()

# ── DB connection ─────────────────────────────────────────
def parse_db_url(url):
    """Parse postgresql://user:pass@host:port/dbname"""
    import urllib.parse
    r = urllib.parse.urlparse(url)
    # urllib.parse automatically decodes percent-encoded characters
    return {
        "host": r.hostname,
        "port": r.port or 5432,
        "database": r.path.lstrip("/"),
        "user": urllib.parse.unquote(r.username) if r.username else r.username,
        "password": urllib.parse.unquote(r.password) if r.password else r.password,
        "ssl_context": True,
    }

def get_conn():
    params = parse_db_url(DATABASE_URL)
    return pg8000.native.Connection(**params)

def run_query(sql, params=None, fetch=True):
    conn = get_conn()
    try:
        if params:
            result = conn.run(sql, *params)
        else:
            result = conn.run(sql)
        if fetch:
            return result
        return None
    finally:
        conn.close()

def require_admin(creds: HTTPBasicCredentials = Depends(security)):
    ok = secrets.compare_digest(creds.username, ADMIN_USER) and \
         secrets.compare_digest(creds.password, ADMIN_PASS)
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Incorrect credentials",
                            headers={"WWW-Authenticate": "Basic"})
    return creds.username

def check_pred(prediction: str, result: str) -> bool:
    p = prediction.lower()
    r = result.lower()
    if "draw" in r or "تعادل" in r:
        return "draw" in p or "تعادل" in p
    team = r.replace(" win","").replace(" wins","").strip()
    return team in p

def recalc_points_db(conn, emp_id: str):
    preds = conn.run("SELECT round, match_name, prediction FROM predictions WHERE emp_id=:e", e=emp_id)
    results_rows = conn.run("SELECT round, match_name, result FROM matches WHERE status='done' AND result IS NOT NULL")
    results = {(r[0], r[1]): r[2] for r in results_rows}
    pts = {1: 0.0, 2: 0.0, 3: 0.0}
    for pr in preds:
        key = (pr[0], pr[1])
        if key in results and pr[2]:
            if check_pred(pr[2], results[key]):
                pts[pr[0]] += 3.0
    conn.run("""INSERT INTO points_cache (emp_id, r1_pts, r2_pts, r3_pts, total)
                VALUES (:e,:r1,:r2,:r3,:t)
                ON CONFLICT (emp_id) DO UPDATE SET
                r1_pts=EXCLUDED.r1_pts, r2_pts=EXCLUDED.r2_pts,
                r3_pts=EXCLUDED.r3_pts, total=EXCLUDED.total""",
             e=emp_id, r1=pts[1], r2=pts[2], r3=pts[3], t=pts[1]+pts[2]+pts[3])

# ── DB init & seed ────────────────────────────────────────
@app.on_event("startup")
def startup():
    conn = get_conn()
    try:
        conn.run("""CREATE TABLE IF NOT EXISTS participants (
            emp_id TEXT PRIMARY KEY, name TEXT NOT NULL, rounds TEXT DEFAULT '[]')""")
        conn.run("""CREATE TABLE IF NOT EXISTS predictions (
            emp_id TEXT, round INTEGER, match_name TEXT, prediction TEXT,
            PRIMARY KEY (emp_id, round, match_name))""")
        conn.run("""CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY, round INTEGER NOT NULL, match_name TEXT NOT NULL,
            result TEXT, status TEXT DEFAULT 'pending', played_at TEXT,
            UNIQUE(round, match_name))""")
        conn.run("""CREATE TABLE IF NOT EXISTS points_cache (
            emp_id TEXT PRIMARY KEY, r1_pts REAL DEFAULT 0,
            r2_pts REAL DEFAULT 0, r3_pts REAL DEFAULT 0, total REAL DEFAULT 0)""")

        count = conn.run("SELECT COUNT(*) FROM participants")[0][0]
        if count > 0:
            return

        seed_path = os.path.join(os.path.dirname(__file__), "..", "data", "seed_data.json")
        if not os.path.exists(seed_path):
            return
        with open(seed_path, encoding="utf-8") as f:
            data = json.load(f)

        for emp_id, info in data["participants"].items():
            conn.run("INSERT INTO participants (emp_id,name,rounds) VALUES (:e,:n,:r) ON CONFLICT DO NOTHING",
                     e=emp_id, n=info["name"], r=json.dumps(info["rounds"]))
            for pred_key, pred_val in info["predictions"].items():
                rnd_str, match = pred_key.split(":", 1)
                rnd = int(rnd_str[1])
                conn.run("INSERT INTO predictions (emp_id,round,match_name,prediction) VALUES (:e,:r,:m,:p) ON CONFLICT DO NOTHING",
                         e=emp_id, r=rnd, m=match, p=pred_val)
            r1p = float(info.get("r1_pts", 0) or 0)
            r2p = float(info.get("r2_pts", 0) or 0)
            r3p = float(info.get("r3_pts", 0) or 0)
            conn.run("""INSERT INTO points_cache (emp_id,r1_pts,r2_pts,r3_pts,total)
                        VALUES (:e,:r1,:r2,:r3,:t) ON CONFLICT (emp_id) DO UPDATE SET
                        r1_pts=EXCLUDED.r1_pts,r2_pts=EXCLUDED.r2_pts,
                        r3_pts=EXCLUDED.r3_pts,total=EXCLUDED.total""",
                     e=emp_id, r1=r1p, r2=r2p, r3=r3p, t=r1p+r2p+r3p)

        for match, result in data["matches"]["R1"]:
            conn.run("INSERT INTO matches (round,match_name,result,status,played_at) VALUES (1,:m,:r,'done','2026-06-01') ON CONFLICT DO NOTHING",
                     m=match, r=result)
        for match, result in data["matches"]["R2"]:
            conn.run("INSERT INTO matches (round,match_name,result,status,played_at) VALUES (2,:m,:r,'done','2026-06-15') ON CONFLICT DO NOTHING",
                     m=match, r=result)
        for match, result in data["matches"]["R3_scored"]:
            conn.run("INSERT INTO matches (round,match_name,result,status,played_at) VALUES (3,:m,:r,'done','2026-06-25') ON CONFLICT DO NOTHING",
                     m=match, r=result)
        for match in data["matches"]["R3_pending"]:
            conn.run("INSERT INTO matches (round,match_name,status) VALUES (3,:m,'pending') ON CONFLICT DO NOTHING", m=match)

        print(f"Seeded {len(data['participants'])} participants.")
    finally:
        conn.close()

# ── PUBLIC ENDPOINTS ──────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    conn = get_conn()
    try:
        total  = conn.run("SELECT COUNT(*) FROM participants")[0][0]
        r1c    = conn.run("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1")[0][0]
        r2c    = conn.run("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2")[0][0]
        r3c    = conn.run("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3")[0][0]
        all3   = conn.run("SELECT COUNT(*) FROM (SELECT emp_id FROM predictions GROUP BY emp_id HAVING COUNT(DISTINCT round)=3) x")[0][0]
        top    = conn.run("SELECT emp_id,r1_pts,r2_pts,r3_pts,total FROM points_cache ORDER BY total DESC LIMIT 1")
        avg    = conn.run("SELECT AVG(total),AVG(r1_pts),AVG(r2_pts),AVG(r3_pts) FROM points_cache")[0]
        mn     = conn.run("SELECT MIN(total) FROM points_cache")[0][0]
        done   = conn.run("SELECT COUNT(*) FROM matches WHERE status='done'")[0][0]
        pending= conn.run("SELECT COUNT(*) FROM matches WHERE status='pending'")[0][0]

        leader_name = ""
        if top:
            t = top[0]
            p = conn.run("SELECT name FROM participants WHERE emp_id=:e", e=t[0])
            leader_name = p[0][0] if p else ""
            top_data = {"name": leader_name, "total": float(t[4]), "r1": float(t[1]), "r2": float(t[2]), "r3": float(t[3])}
        else:
            top_data = {"name": "", "total": 0, "r1": 0, "r2": 0, "r3": 0}

        combos = {}
        for k, q in [
            ("r1only","SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)"),
            ("r2only","SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)"),
            ("r3only","SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)"),
            ("r1r2","SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=2 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)"),
            ("r1r3","SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)"),
            ("r2r3","SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=2 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1)"),
        ]:
            combos[k] = conn.run(q)[0][0]
        combos["all3"] = all3

        totals = [r[0] for r in conn.run("SELECT total FROM points_cache")]
        bins = [0,30,40,50,60,70,80,90,100,110,120,130,500]
        bl   = ['0-30','31-40','41-50','51-60','61-70','71-80','81-90','91-100','101-110','111-120','121-130','130+']
        dist = [0]*len(bl)
        for t in totals:
            for i in range(len(bins)-1):
                if (i==0 and t==0) or (bins[i]<t<=bins[i+1]):
                    dist[i]+=1; break

        return {"total_participants":total,"r1_participants":r1c,"r2_participants":r2c,"r3_participants":r3c,
                "all3_participants":all3,"leader":top_data,
                "avg_total":round(float(avg[0] or 0),1),"avg_r1":round(float(avg[1] or 0),1),
                "avg_r2":round(float(avg[2] or 0),1),"avg_r3":round(float(avg[3] or 0),1),
                "min_total":float(mn or 0),"matches_done":done,"matches_pending":pending,
                "combos":combos,"dist_labels":bl,"dist_values":dist}
    finally:
        conn.close()

@app.get("/api/leaderboard")
def get_leaderboard(page: int=1, per_page: int=20, search: str="", round_filter: str="all"):
    conn = get_conn()
    try:
        rows = conn.run("""SELECT pc.emp_id,p.name,pc.r1_pts,pc.r2_pts,pc.r3_pts,pc.total,p.rounds
                           FROM points_cache pc JOIN participants p ON pc.emp_id=p.emp_id
                           ORDER BY pc.total DESC, p.name ASC""")
        results, rank, prev_total = [], 0, None
        for r in rows:
            rounds = json.loads(r[6])
            if round_filter=="r1" and 1 not in rounds: continue
            if round_filter=="r2" and 2 not in rounds: continue
            if round_filter=="r3" and 3 not in rounds: continue
            if round_filter=="all3" and not all(x in rounds for x in [1,2,3]): continue
            if search and search.lower() not in r[1].lower() and search not in r[0]: continue
            if r[5]!=prev_total: rank=len(results)+1; prev_total=r[5]
            results.append({"rank":rank,"emp_id":r[0],"name":r[1],
                            "r1":float(r[2]),"r2":float(r[3]),"r3":float(r[4]),"total":float(r[5]),
                            "p1":1 in rounds,"p2":2 in rounds,"p3":3 in rounds})
        start=(page-1)*per_page
        return {"data":results[start:start+per_page],"total":len(results),"page":page,"per_page":per_page}
    finally:
        conn.close()

@app.get("/api/matches")
def get_matches():
    conn = get_conn()
    try:
        rows = conn.run("SELECT id,round,match_name,result,status,played_at FROM matches ORDER BY round,id")
        return [{"id":r[0],"round":r[1],"match_name":r[2],"result":r[3],"status":r[4],"played_at":r[5]} for r in rows]
    finally:
        conn.close()

@app.get("/api/events")
async def sse_events(request: Request):
    q = broadcaster.subscribe()
    async def event_stream():
        yield "data: {\"type\":\"connected\"}\n\n"
        try:
            while True:
                if await request.is_disconnected(): break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=25)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            broadcaster.unsubscribe(q)
    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# ── ADMIN ENDPOINTS ───────────────────────────────────────
class MatchResult(BaseModel):
    round: int
    match_name: str
    result: str

@app.post("/api/admin/result")
async def submit_result(payload: MatchResult, _=Depends(require_admin)):
    conn = get_conn()
    try:
        ex = conn.run("SELECT id FROM matches WHERE round=:r AND match_name=:m", r=payload.round, m=payload.match_name)
        if not ex:
            raise HTTPException(404, f"Match not found")
        conn.run("UPDATE matches SET result=:res,status='done',played_at=:t WHERE round=:r AND match_name=:m",
                 res=payload.result, t=datetime.utcnow().isoformat(), r=payload.round, m=payload.match_name)
        participants = conn.run("SELECT DISTINCT emp_id FROM predictions WHERE round=:r", r=payload.round)
        for p in participants:
            recalc_points_db(conn, p[0])
    finally:
        conn.close()
    await broadcaster.broadcast({"type":"update","match":payload.match_name,"result":payload.result,"round":payload.round})
    return {"status":"ok","match":payload.match_name,"result":payload.result,"updated":len(participants)}

@app.post("/api/admin/result/batch")
async def submit_results_batch(results: list[MatchResult], _=Depends(require_admin)):
    updated = []
    conn = get_conn()
    try:
        for payload in results:
            ex = conn.run("SELECT id FROM matches WHERE round=:r AND match_name=:m", r=payload.round, m=payload.match_name)
            if not ex: continue
            conn.run("UPDATE matches SET result=:res,status='done',played_at=:t WHERE round=:r AND match_name=:m",
                     res=payload.result, t=datetime.utcnow().isoformat(), r=payload.round, m=payload.match_name)
            updated.append(payload.match_name)
        all_ids = conn.run("SELECT emp_id FROM participants")
        for p in all_ids:
            recalc_points_db(conn, p[0])
    finally:
        conn.close()
    await broadcaster.broadcast({"type":"update","updated":updated})
    return {"status":"ok","updated":updated}

@app.get("/api/admin/matches/pending")
def get_pending(_=Depends(require_admin)):
    conn = get_conn()
    try:
        rows = conn.run("SELECT id,round,match_name,result,status,played_at FROM matches WHERE status='pending' ORDER BY round,id")
        return [{"id":r[0],"round":r[1],"match_name":r[2],"result":r[3],"status":r[4],"played_at":r[5]} for r in rows]
    finally:
        conn.close()

@app.get("/api/admin/participant/{emp_id}")
def get_participant_detail(emp_id: str, _=Depends(require_admin)):
    conn = get_conn()
    try:
        p = conn.run("SELECT emp_id,name,rounds FROM participants WHERE emp_id=:e", e=emp_id)
        if not p: raise HTTPException(404,"Participant not found")
        preds = conn.run("SELECT round,match_name,prediction FROM predictions WHERE emp_id=:e ORDER BY round", e=emp_id)
        pts = conn.run("SELECT r1_pts,r2_pts,r3_pts,total FROM points_cache WHERE emp_id=:e", e=emp_id)
        match_res = conn.run("SELECT round,match_name,result FROM matches WHERE status='done'")
        mr = {(r[0],r[1]):r[2] for r in match_res}
        pred_detail = []
        for pr in preds:
            res = mr.get((pr[0],pr[1]))
            correct = check_pred(pr[2],res) if res else None
            pred_detail.append({"round":pr[0],"match":pr[1],"prediction":pr[2],"result":res,
                                 "correct":correct,"points":3 if correct else 0})
        pts_row = pts[0] if pts else (0,0,0,0)
        return {"emp_id":emp_id,"name":p[0][1],"rounds":json.loads(p[0][2]),
                "r1_pts":float(pts_row[0]),"r2_pts":float(pts_row[1]),
                "r3_pts":float(pts_row[2]),"total":float(pts_row[3]),
                "predictions":pred_detail}
    finally:
        conn.close()

@app.post("/api/admin/reset-and-reseed")
def reset_reseed(_=Depends(require_admin)):
    conn = get_conn()
    try:
        conn.run("TRUNCATE TABLE points_cache, predictions, matches, participants RESTART IDENTITY CASCADE")
    finally:
        conn.close()
    startup()
    return {"status":"ok","message":"Database reset and reseeded successfully"}

# ── Serve frontend ────────────────────────────────────────
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
