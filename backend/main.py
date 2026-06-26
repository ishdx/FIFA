from fastapi import FastAPI, HTTPException, Depends, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2, psycopg2.extras, json, os, secrets, asyncio
from contextlib import contextmanager
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
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            if q in self._queues:
                self._queues.remove(q)

broadcaster = EventBroadcaster()

app = FastAPI(title="FIFA WC2026 Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_USER   = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS   = os.environ.get("ADMIN_PASS", "smi2026")
security     = HTTPBasic()

# ── DB helpers ────────────────────────────────────────────
@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def dict_fetchall(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def dict_fetchone(cursor):
    cols = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    return dict(zip(cols, row)) if row else None

def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            emp_id TEXT PRIMARY KEY,
            name   TEXT NOT NULL,
            rounds TEXT DEFAULT '[]'
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            emp_id     TEXT,
            round      INTEGER,
            match_name TEXT,
            prediction TEXT,
            PRIMARY KEY (emp_id, round, match_name)
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id         SERIAL PRIMARY KEY,
            round      INTEGER NOT NULL,
            match_name TEXT NOT NULL,
            result     TEXT,
            status     TEXT DEFAULT 'pending',
            played_at  TEXT,
            UNIQUE(round, match_name)
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS points_cache (
            emp_id   TEXT PRIMARY KEY,
            r1_pts   REAL DEFAULT 0,
            r2_pts   REAL DEFAULT 0,
            r3_pts   REAL DEFAULT 0,
            total    REAL DEFAULT 0
        )""")

def require_admin(creds: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(creds.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(creds.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
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

def recalc_points(db, emp_id: str):
    cur = db.cursor()
    cur.execute("SELECT round, match_name, prediction FROM predictions WHERE emp_id=%s", (emp_id,))
    preds = dict_fetchall(cur)
    cur.execute("SELECT round, match_name, result FROM matches WHERE status='done' AND result IS NOT NULL")
    results = {(r["round"], r["match_name"]): r["result"] for r in dict_fetchall(cur)}
    pts = {1: 0.0, 2: 0.0, 3: 0.0}
    for pr in preds:
        key = (pr["round"], pr["match_name"])
        if key in results and pr["prediction"]:
            if check_pred(pr["prediction"], results[key]):
                pts[pr["round"]] += 3.0
    cur.execute("""
        INSERT INTO points_cache (emp_id, r1_pts, r2_pts, r3_pts, total)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (emp_id) DO UPDATE SET
            r1_pts=EXCLUDED.r1_pts, r2_pts=EXCLUDED.r2_pts,
            r3_pts=EXCLUDED.r3_pts, total=EXCLUDED.total
    """, (emp_id, pts[1], pts[2], pts[3], pts[1]+pts[2]+pts[3]))

def recalc_all(db):
    cur = db.cursor()
    cur.execute("SELECT emp_id FROM participants")
    ids = [r[0] for r in cur.fetchall()]
    for eid in ids:
        recalc_points(db, eid)

def seed_db():
    seed_path = os.path.join(os.path.dirname(__file__), "..", "data", "seed_data.json")
    if not os.path.exists(seed_path):
        return
    with open(seed_path, encoding="utf-8") as f:
        data = json.load(f)
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM participants")
        if cur.fetchone()[0] > 0:
            return  # already seeded
        for emp_id, info in data["participants"].items():
            cur.execute("INSERT INTO participants (emp_id, name, rounds) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (emp_id, info["name"], json.dumps(info["rounds"])))
            for pred_key, pred_val in info["predictions"].items():
                rnd_str, match = pred_key.split(":", 1)
                rnd = int(rnd_str[1])
                cur.execute("""INSERT INTO predictions (emp_id, round, match_name, prediction)
                               VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                            (emp_id, rnd, match, pred_val))
        for match, result in data["matches"]["R1"]:
            cur.execute("""INSERT INTO matches (round, match_name, result, status, played_at)
                           VALUES (%s,%s,%s,'done',%s) ON CONFLICT DO NOTHING""",
                        (1, match, result, "2026-06-01"))
        for match, result in data["matches"]["R2"]:
            cur.execute("""INSERT INTO matches (round, match_name, result, status, played_at)
                           VALUES (%s,%s,%s,'done',%s) ON CONFLICT DO NOTHING""",
                        (2, match, result, "2026-06-15"))
        for match, result in data["matches"]["R3_scored"]:
            cur.execute("""INSERT INTO matches (round, match_name, result, status, played_at)
                           VALUES (%s,%s,%s,'done',%s) ON CONFLICT DO NOTHING""",
                        (3, match, result, "2026-06-25"))
        for match in data["matches"]["R3_pending"]:
            cur.execute("""INSERT INTO matches (round, match_name, status)
                           VALUES (%s,%s,'pending') ON CONFLICT DO NOTHING""",
                        (3, match))
        recalc_all(db)
        print(f"Seeded {len(data['participants'])} participants.")

@app.on_event("startup")
def startup():
    init_db()
    seed_db()

# ── PUBLIC ENDPOINTS ──────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM participants"); total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1"); r1 = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2"); r2 = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3"); r3 = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM (
            SELECT emp_id FROM predictions GROUP BY emp_id HAVING COUNT(DISTINCT round)=3
        ) x"""); all3 = cur.fetchone()[0]
        cur.execute("SELECT emp_id, r1_pts, r2_pts, r3_pts, total FROM points_cache ORDER BY total DESC LIMIT 1")
        top = dict_fetchone(cur)
        cur.execute("SELECT AVG(total), AVG(r1_pts), AVG(r2_pts), AVG(r3_pts) FROM points_cache")
        avg = cur.fetchone()
        cur.execute("SELECT MIN(total) FROM points_cache"); mn = cur.fetchone()[0]
        leader_name = ""
        if top:
            cur.execute("SELECT name FROM participants WHERE emp_id=%s", (top["emp_id"],))
            p = cur.fetchone()
            leader_name = p[0] if p else ""
        cur.execute("SELECT COUNT(*) FROM matches WHERE status='pending'"); pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM matches WHERE status='done'"); done = cur.fetchone()[0]

        combos = {}
        combo_queries = {
            "r1only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r2only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r3only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)",
            "r1r2":   "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=2 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r1r3":   "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)",
            "r2r3":   "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=2 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1)",
        }
        for k, q in combo_queries.items():
            cur.execute(q); combos[k] = cur.fetchone()[0]
        combos["all3"] = all3

        cur.execute("SELECT total FROM points_cache")
        totals = [r[0] for r in cur.fetchall()]
        bins = [0,30,40,50,60,70,80,90,100,110,120,130,500]
        bin_labels = ['0-30','31-40','41-50','51-60','61-70','71-80','81-90','91-100','101-110','111-120','121-130','130+']
        dist = [0]*len(bin_labels)
        for t in totals:
            for i in range(len(bins)-1):
                if (i==0 and t==0) or (bins[i] < t <= bins[i+1]):
                    dist[i] += 1; break

        return {
            "total_participants": total,
            "r1_participants": r1, "r2_participants": r2, "r3_participants": r3,
            "all3_participants": all3,
            "leader": {"name": leader_name, "total": top["total"] if top else 0,
                       "r1": top["r1_pts"] if top else 0, "r2": top["r2_pts"] if top else 0,
                       "r3": top["r3_pts"] if top else 0},
            "avg_total": round(avg[0] or 0, 1),
            "avg_r1": round(avg[1] or 0, 1),
            "avg_r2": round(avg[2] or 0, 1),
            "avg_r3": round(avg[3] or 0, 1),
            "min_total": mn or 0,
            "matches_done": done,
            "matches_pending": pending,
            "combos": combos,
            "dist_labels": bin_labels,
            "dist_values": dist,
        }

@app.get("/api/leaderboard")
def get_leaderboard(page: int = 1, per_page: int = 20, search: str = "", round_filter: str = "all"):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT pc.emp_id, p.name, pc.r1_pts, pc.r2_pts, pc.r3_pts, pc.total, p.rounds
            FROM points_cache pc
            JOIN participants p ON pc.emp_id=p.emp_id
            ORDER BY pc.total DESC, p.name ASC
        """)
        rows = dict_fetchall(cur)
        results = []
        rank = 0
        prev_total = None
        for r in rows:
            rounds = json.loads(r["rounds"])
            if round_filter == "r1" and 1 not in rounds: continue
            if round_filter == "r2" and 2 not in rounds: continue
            if round_filter == "r3" and 3 not in rounds: continue
            if round_filter == "all3" and not all(x in rounds for x in [1,2,3]): continue
            if search and search.lower() not in r["name"].lower() and search not in r["emp_id"]: continue
            if r["total"] != prev_total:
                rank = len(results) + 1
                prev_total = r["total"]
            results.append({
                "rank": rank, "emp_id": r["emp_id"], "name": r["name"],
                "r1": r["r1_pts"], "r2": r["r2_pts"], "r3": r["r3_pts"],
                "total": r["total"],
                "p1": 1 in rounds, "p2": 2 in rounds, "p3": 3 in rounds,
            })
        total_count = len(results)
        start = (page-1)*per_page
        return {"data": results[start:start+per_page], "total": total_count, "page": page, "per_page": per_page}

@app.get("/api/matches")
def get_matches():
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM matches ORDER BY round, id")
        return dict_fetchall(cur)

@app.get("/api/events")
async def sse_events(request: Request):
    q = broadcaster.subscribe()
    async def event_stream():
        yield "data: {\"type\":\"connected\"}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=25)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            broadcaster.unsubscribe(q)
    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ── ADMIN ENDPOINTS ───────────────────────────────────────
class MatchResult(BaseModel):
    round: int
    match_name: str
    result: str

@app.post("/api/admin/result")
async def submit_result(payload: MatchResult, _=Depends(require_admin)):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT id FROM matches WHERE round=%s AND match_name=%s",
                    (payload.round, payload.match_name))
        if not cur.fetchone():
            raise HTTPException(404, f"Match not found: R{payload.round} {payload.match_name}")
        cur.execute("""UPDATE matches SET result=%s, status='done', played_at=%s
                       WHERE round=%s AND match_name=%s""",
                    (payload.result, datetime.utcnow().isoformat(), payload.round, payload.match_name))
        cur.execute("SELECT DISTINCT emp_id FROM predictions WHERE round=%s", (payload.round,))
        participants = [r[0] for r in cur.fetchall()]
        for eid in participants:
            recalc_points(db, eid)
    await broadcaster.broadcast({"type": "update", "match": payload.match_name,
                                  "result": payload.result, "round": payload.round})
    return {"status": "ok", "match": payload.match_name, "result": payload.result,
            "updated": len(participants)}

@app.post("/api/admin/result/batch")
async def submit_results_batch(results: list[MatchResult], _=Depends(require_admin)):
    updated = []
    with get_db() as db:
        cur = db.cursor()
        for payload in results:
            cur.execute("SELECT id FROM matches WHERE round=%s AND match_name=%s",
                        (payload.round, payload.match_name))
            if not cur.fetchone():
                continue
            cur.execute("""UPDATE matches SET result=%s, status='done', played_at=%s
                           WHERE round=%s AND match_name=%s""",
                        (payload.result, datetime.utcnow().isoformat(), payload.round, payload.match_name))
            updated.append(payload.match_name)
        recalc_all(db)
    await broadcaster.broadcast({"type": "update", "updated": updated})
    return {"status": "ok", "updated": updated}

@app.get("/api/admin/matches/pending")
def get_pending(_=Depends(require_admin)):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM matches WHERE status='pending' ORDER BY round, id")
        return dict_fetchall(cur)

@app.get("/api/admin/participant/{emp_id}")
def get_participant_detail(emp_id: str, _=Depends(require_admin)):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM participants WHERE emp_id=%s", (emp_id,))
        p = dict_fetchone(cur)
        if not p:
            raise HTTPException(404, "Participant not found")
        cur.execute("SELECT round, match_name, prediction FROM predictions WHERE emp_id=%s ORDER BY round", (emp_id,))
        preds = dict_fetchall(cur)
        cur.execute("SELECT * FROM points_cache WHERE emp_id=%s", (emp_id,))
        pts = dict_fetchone(cur)
        cur.execute("SELECT round, match_name, result FROM matches WHERE status='done'")
        match_results = {(m["round"], m["match_name"]): m["result"] for m in dict_fetchall(cur)}
        pred_detail = []
        for pred in preds:
            res = match_results.get((pred["round"], pred["match_name"]))
            correct = check_pred(pred["prediction"], res) if res else None
            pred_detail.append({
                "round": pred["round"], "match": pred["match_name"],
                "prediction": pred["prediction"], "result": res,
                "correct": correct, "points": 3 if correct else 0
            })
        return {
            "emp_id": emp_id, "name": p["name"],
            "rounds": json.loads(p["rounds"]),
            "r1_pts": pts["r1_pts"] if pts else 0,
            "r2_pts": pts["r2_pts"] if pts else 0,
            "r3_pts": pts["r3_pts"] if pts else 0,
            "total": pts["total"] if pts else 0,
            "predictions": pred_detail,
        }

# ── Serve frontend ────────────────────────────────────────
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
