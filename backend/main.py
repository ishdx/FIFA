from fastapi import FastAPI, HTTPException, Depends, status, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import pg8000.native, json, os, secrets, asyncio, urllib.parse, traceback
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

# ── DB ────────────────────────────────────────────────────
def get_conn():
    r = urllib.parse.urlparse(DATABASE_URL)
    return pg8000.native.Connection(
        host=r.hostname,
        port=r.port or 5432,
        database=r.path.lstrip("/"),
        user=urllib.parse.unquote(r.username or ""),
        password=urllib.parse.unquote(r.password or ""),
        ssl_context=True,
    )

def safe_run(conn, sql, **kwargs):
    try:
        return conn.run(sql, **kwargs)
    except Exception as e:
        print(f"SQL error: {e}\nSQL: {sql}")
        return []

# ── Auth ──────────────────────────────────────────────────
def require_admin(x_admin_token: str = Header(default="")):
    import base64
    try:
        decoded = base64.b64decode(x_admin_token).decode()
        user, password = decoded.split(":", 1)
        if secrets.compare_digest(user, ADMIN_USER) and secrets.compare_digest(password, ADMIN_PASS):
            return user
    except Exception:
        pass
    raise HTTPException(status_code=401, detail="Unauthorized")

# ── Scoring ───────────────────────────────────────────────
def check_pred(prediction: str, result: str) -> bool:
    if not prediction or not result:
        return False
    p_en = prediction.lower().split("|")[-1].strip()
    r_en = result.lower().split("|")[-1].strip()
    return p_en == r_en

def recalc_all_points(conn):
    results_rows = safe_run(conn,
        "SELECT round, match_name, result FROM matches WHERE status='done' AND result IS NOT NULL")
    results = {(r[0], r[1]): r[2] for r in results_rows}
    all_ids = safe_run(conn, "SELECT emp_id FROM participants")
    for row in all_ids:
        emp_id = row[0]
        preds = safe_run(conn,
            "SELECT round, match_name, prediction FROM predictions WHERE emp_id=:e", e=emp_id)
        pts = {1: 0.0, 2: 0.0, 3: 0.0}
        for pr in preds:
            key = (pr[0], pr[1])
            if key in results and pr[2] and check_pred(pr[2], results[key]):
                pts[pr[0]] += 3.0
        bonus_row = safe_run(conn, "SELECT bonus_pts FROM points_cache WHERE emp_id=:e", e=emp_id)
        bonus_pts = float(bonus_row[0][0]) if bonus_row else 0.0
        total = pts[1] + pts[2] + pts[3] + bonus_pts
        safe_run(conn, """INSERT INTO points_cache (emp_id,r1_pts,r2_pts,r3_pts,bonus_pts,total)
                    VALUES (:e,:r1,:r2,:r3,:b,:t)
                    ON CONFLICT (emp_id) DO UPDATE SET
                    r1_pts=EXCLUDED.r1_pts, r2_pts=EXCLUDED.r2_pts,
                    r3_pts=EXCLUDED.r3_pts, total=EXCLUDED.total""",
                 e=emp_id, r1=pts[1], r2=pts[2], r3=pts[3], b=bonus_pts, t=total)

# ── DB init ───────────────────────────────────────────────
def init_db(conn):
    safe_run(conn, """CREATE TABLE IF NOT EXISTS participants (
        emp_id TEXT PRIMARY KEY, name TEXT NOT NULL, rounds TEXT DEFAULT '[]')""")
    safe_run(conn, """CREATE TABLE IF NOT EXISTS predictions (
        emp_id TEXT, round INTEGER, match_name TEXT, prediction TEXT,
        PRIMARY KEY (emp_id, round, match_name))""")
    safe_run(conn, """CREATE TABLE IF NOT EXISTS matches (
        id SERIAL PRIMARY KEY, round INTEGER NOT NULL, match_name TEXT NOT NULL,
        result TEXT, status TEXT DEFAULT 'pending', played_at TEXT,
        UNIQUE(round, match_name))""")
    safe_run(conn, """CREATE TABLE IF NOT EXISTS points_cache (
        emp_id TEXT PRIMARY KEY,
        r1_pts REAL DEFAULT 0, r2_pts REAL DEFAULT 0,
        r3_pts REAL DEFAULT 0, total REAL DEFAULT 0)""")
    # Add new columns safely
    safe_run(conn, "ALTER TABLE points_cache ADD COLUMN IF NOT EXISTS bonus_pts REAL DEFAULT 0")
    safe_run(conn, "ALTER TABLE matches ADD COLUMN IF NOT EXISTS options TEXT DEFAULT '[]'")

# ── Seed ─────────────────────────────────────────────────
def seed_db(conn):
    count = conn.run("SELECT COUNT(*) FROM participants")[0][0]
    if count > 0:
        print(f"Already seeded ({count} participants), skipping.")
        return

    seed_path = os.path.join(os.path.dirname(__file__), "..", "data", "seed_data.json")
    if not os.path.exists(seed_path):
        print("No seed_data.json found.")
        return

    with open(seed_path, encoding="utf-8") as f:
        data = json.load(f)

    match_options = data.get("match_options", {})

    for emp_id, info in data["participants"].items():
        safe_run(conn,
            "INSERT INTO participants (emp_id,name,rounds) VALUES (:e,:n,:r) ON CONFLICT DO NOTHING",
            e=emp_id, n=info["name"], r=json.dumps(info["rounds"]))
        for pred_key, pred_val in info["predictions"].items():
            rnd_str, match = pred_key.split(":", 1)
            rnd = int(rnd_str[1])
            safe_run(conn,
                "INSERT INTO predictions (emp_id,round,match_name,prediction) VALUES (:e,:r,:m,:p) ON CONFLICT DO NOTHING",
                e=emp_id, r=rnd, m=match, p=pred_val)
        safe_run(conn,
            "INSERT INTO points_cache (emp_id,r1_pts,r2_pts,r3_pts,total) VALUES (:e,0,0,0,0) ON CONFLICT DO NOTHING",
            e=emp_id)

    for rnd, key in [(1,"R1"),(2,"R2"),(3,"R3")]:
        for match in data["matches"][key]:
            opts = match_options.get(f"{key}:{match}", [])
            safe_run(conn,
                "INSERT INTO matches (round,match_name,status,options) VALUES (:r,:m,'pending',:o) ON CONFLICT DO NOTHING",
                r=rnd, m=match, o=json.dumps(opts, ensure_ascii=False))

    print(f"Seeded {len(data['participants'])} participants, all points = 0.")

@app.on_event("startup")
def startup():
    try:
        conn = get_conn()
        try:
            init_db(conn)
            seed_db(conn)
        finally:
            conn.close()
    except Exception as e:
        print(f"STARTUP ERROR: {e}")
        traceback.print_exc()

# ── PUBLIC ENDPOINTS ──────────────────────────────────────
@app.get("/api/health")
def health():
    try:
        conn = get_conn()
        conn.run("SELECT 1")
        try:
            count = conn.run("SELECT COUNT(*) FROM participants")[0][0]
            matches = conn.run("SELECT COUNT(*) FROM matches")[0][0]
        except:
            count, matches = 0, 0
        conn.close()
        return {"status": "ok", "db": "connected", "participants": count, "matches": matches}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/api/stats")
def get_stats():
    conn = get_conn()
    try:
        total   = safe_run(conn, "SELECT COUNT(*) FROM participants") or [[0]]
        total   = total[0][0]
        r1c     = (safe_run(conn, "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1") or [[0]])[0][0]
        r2c     = (safe_run(conn, "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2") or [[0]])[0][0]
        r3c     = (safe_run(conn, "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3") or [[0]])[0][0]
        all3    = (safe_run(conn, "SELECT COUNT(*) FROM (SELECT emp_id FROM predictions GROUP BY emp_id HAVING COUNT(DISTINCT round)=3) x") or [[0]])[0][0]
        top     = safe_run(conn, "SELECT emp_id,r1_pts,r2_pts,r3_pts,COALESCE(bonus_pts,0),total FROM points_cache ORDER BY total DESC LIMIT 1") or []
        avg_row = safe_run(conn, "SELECT AVG(total),AVG(r1_pts),AVG(r2_pts),AVG(r3_pts) FROM points_cache") or [[0,0,0,0]]
        avg     = avg_row[0]
        mn      = (safe_run(conn, "SELECT MIN(total) FROM points_cache") or [[0]])[0][0]
        done    = (safe_run(conn, "SELECT COUNT(*) FROM matches WHERE status='done'") or [[0]])[0][0]
        pending = (safe_run(conn, "SELECT COUNT(*) FROM matches WHERE status='pending'") or [[0]])[0][0]

        top_data = {"name":"","total":0,"r1":0,"r2":0,"r3":0,"bonus":0}
        if top:
            t = top[0]
            p = safe_run(conn, "SELECT name FROM participants WHERE emp_id=:e", e=t[0])
            top_data = {"name":p[0][0] if p else "","total":float(t[5] or 0),
                        "r1":float(t[1] or 0),"r2":float(t[2] or 0),"r3":float(t[3] or 0),"bonus":float(t[4] or 0)}

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
        rows = conn.run("""SELECT pc.emp_id,p.name,pc.r1_pts,pc.r2_pts,pc.r3_pts,
                           COALESCE(pc.bonus_pts,0),pc.total,p.rounds
                           FROM points_cache pc JOIN participants p ON pc.emp_id=p.emp_id
                           ORDER BY pc.total DESC, p.name ASC""")
        results, rank, prev_total = [], 0, None
        for r in rows:
            rounds = json.loads(r[7])
            if round_filter=="r1" and 1 not in rounds: continue
            if round_filter=="r2" and 2 not in rounds: continue
            if round_filter=="r3" and 3 not in rounds: continue
            if round_filter=="all3" and not all(x in rounds for x in [1,2,3]): continue
            if search and search.lower() not in r[1].lower() and search not in r[0]: continue
            if r[6]!=prev_total: rank=len(results)+1; prev_total=r[6]
            results.append({"rank":rank,"emp_id":r[0],"name":r[1],
                            "r1":float(r[2]),"r2":float(r[3]),"r3":float(r[4]),
                            "bonus":float(r[5]),"total":float(r[6]),
                            "p1":1 in rounds,"p2":2 in rounds,"p3":3 in rounds})
        start=(page-1)*per_page
        return {"data":results[start:start+per_page],"total":len(results),"page":page,"per_page":per_page}
    finally:
        conn.close()

@app.get("/api/matches")
def get_matches():
    conn = get_conn()
    try:
        rows = conn.run("SELECT id,round,match_name,result,status,played_at,COALESCE(options,'[]') FROM matches ORDER BY round,id")
        return [{"id":r[0],"round":r[1],"match_name":r[2],"result":r[3],
                 "status":r[4],"played_at":r[5],"options":json.loads(r[6])} for r in rows]
    except Exception as e:
        raise HTTPException(500, str(e))
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
@app.get("/api/admin/verify")
def verify_admin(_=Depends(require_admin)):
    return {"status": "ok"}

class MatchResult(BaseModel):
    round: int
    match_name: str
    result: str

@app.post("/api/admin/result")
async def submit_result(payload: MatchResult, _=Depends(require_admin)):
    conn = get_conn()
    try:
        ex = conn.run("SELECT id FROM matches WHERE round=:r AND match_name=:m",
                      r=payload.round, m=payload.match_name)
        if not ex:
            raise HTTPException(404, "Match not found")
        conn.run("UPDATE matches SET result=:res,status='done',played_at=:t WHERE round=:r AND match_name=:m",
                 res=payload.result, t=datetime.utcnow().isoformat(),
                 r=payload.round, m=payload.match_name)
        recalc_all_points(conn)
    finally:
        conn.close()
    await broadcaster.broadcast({"type":"update","match":payload.match_name,
                                  "result":payload.result,"round":payload.round})
    return {"status":"ok","match":payload.match_name,"result":payload.result}

class BonusPoints(BaseModel):
    emp_id: str
    bonus_pts: float

@app.post("/api/admin/bonus")
async def set_bonus(payload: BonusPoints, _=Depends(require_admin)):
    conn = get_conn()
    try:
        ex = conn.run("SELECT emp_id FROM participants WHERE emp_id=:e", e=payload.emp_id)
        if not ex:
            raise HTTPException(404, "Participant not found")
        conn.run("""UPDATE points_cache SET bonus_pts=:b,
                    total=r1_pts+r2_pts+r3_pts+:b WHERE emp_id=:e""",
                 b=payload.bonus_pts, e=payload.emp_id)
    finally:
        conn.close()
    await broadcaster.broadcast({"type":"update","bonus":True})
    return {"status":"ok","emp_id":payload.emp_id,"bonus_pts":payload.bonus_pts}

@app.get("/api/admin/matches/pending")
def get_pending(_=Depends(require_admin)):
    conn = get_conn()
    try:
        rows = conn.run("SELECT id,round,match_name,result,status,COALESCE(options,'[]') FROM matches WHERE status='pending' ORDER BY round,id")
        return [{"id":r[0],"round":r[1],"match_name":r[2],"result":r[3],
                 "status":r[4],"options":json.loads(r[5])} for r in rows]
    finally:
        conn.close()

@app.get("/api/admin/participant/{emp_id}")
def get_participant_detail(emp_id: str, _=Depends(require_admin)):
    conn = get_conn()
    try:
        p = conn.run("SELECT emp_id,name,rounds FROM participants WHERE emp_id=:e", e=emp_id)
        if not p: raise HTTPException(404,"Participant not found")
        preds = conn.run("SELECT round,match_name,prediction FROM predictions WHERE emp_id=:e ORDER BY round", e=emp_id)
        pts = conn.run("SELECT r1_pts,r2_pts,r3_pts,COALESCE(bonus_pts,0),total FROM points_cache WHERE emp_id=:e", e=emp_id)
        match_res = conn.run("SELECT round,match_name,result FROM matches WHERE status='done'")
        mr = {(r[0],r[1]):r[2] for r in match_res}
        pred_detail = []
        for pr in preds:
            res = mr.get((pr[0],pr[1]))
            correct = check_pred(pr[2], res) if res else None
            pred_detail.append({"round":pr[0],"match":pr[1],"prediction":pr[2],
                                 "result":res,"correct":correct,"points":3 if correct else 0})
        pts_row = pts[0] if pts else (0,0,0,0,0)
        return {"emp_id":emp_id,"name":p[0][1],"rounds":json.loads(p[0][2]),
                "r1_pts":float(pts_row[0]),"r2_pts":float(pts_row[1]),
                "r3_pts":float(pts_row[2]),"bonus_pts":float(pts_row[3]),
                "total":float(pts_row[4]),"predictions":pred_detail}
    finally:
        conn.close()

@app.post("/api/admin/reset-and-reseed")
async def reset_reseed(_=Depends(require_admin)):
    import threading
    def do_reset():
        try:
            conn = get_conn()
            try:
                conn.run("DROP TABLE IF EXISTS points_cache CASCADE")
                conn.run("DROP TABLE IF EXISTS predictions CASCADE")
                conn.run("DROP TABLE IF EXISTS matches CASCADE")
                conn.run("DROP TABLE IF EXISTS participants CASCADE")
            finally:
                conn.close()
            conn2 = get_conn()
            try:
                init_db(conn2)
                seed_db(conn2)
                print("Reset and reseed complete!")
            finally:
                conn2.close()
        except Exception as e:
            traceback.print_exc()
            print(f"Reset failed: {e}")
    threading.Thread(target=do_reset, daemon=True).start()
    return {"status":"ok","message":"Reset started in background — check /api/health in 30s"}

# ── Serve frontend ────────────────────────────────────────
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
