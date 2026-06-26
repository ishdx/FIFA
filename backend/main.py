from fastapi import FastAPI, HTTPException, Depends, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import asyncpg, json, os, secrets, asyncio
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

# ── DB pool ───────────────────────────────────────────────
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await init_db()
    await seed_db()

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

# ── Auth ──────────────────────────────────────────────────
def require_admin(creds: HTTPBasicCredentials = Depends(security)):
    ok = secrets.compare_digest(creds.username, ADMIN_USER) and \
         secrets.compare_digest(creds.password, ADMIN_PASS)
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Incorrect credentials",
                            headers={"WWW-Authenticate": "Basic"})
    return creds.username

# ── DB init ───────────────────────────────────────────────
async def init_db():
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            emp_id TEXT PRIMARY KEY,
            name   TEXT NOT NULL,
            rounds TEXT DEFAULT '[]'
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            emp_id     TEXT,
            round      INTEGER,
            match_name TEXT,
            prediction TEXT,
            PRIMARY KEY (emp_id, round, match_name)
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id         SERIAL PRIMARY KEY,
            round      INTEGER NOT NULL,
            match_name TEXT NOT NULL,
            result     TEXT,
            status     TEXT DEFAULT 'pending',
            played_at  TEXT,
            UNIQUE(round, match_name)
        )""")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS points_cache (
            emp_id   TEXT PRIMARY KEY,
            r1_pts   REAL DEFAULT 0,
            r2_pts   REAL DEFAULT 0,
            r3_pts   REAL DEFAULT 0,
            total    REAL DEFAULT 0
        )""")

async def seed_db():
    seed_path = os.path.join(os.path.dirname(__file__), "..", "data", "seed_data.json")
    if not os.path.exists(seed_path):
        return
    with open(seed_path, encoding="utf-8") as f:
        data = json.load(f)
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM participants")
        if count > 0:
            return  # already seeded
        # Insert participants, predictions, points
        for emp_id, info in data["participants"].items():
            await conn.execute(
                "INSERT INTO participants (emp_id, name, rounds) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                emp_id, info["name"], json.dumps(info["rounds"]))
            for pred_key, pred_val in info["predictions"].items():
                rnd_str, match = pred_key.split(":", 1)
                rnd = int(rnd_str[1])
                await conn.execute(
                    "INSERT INTO predictions (emp_id, round, match_name, prediction) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    emp_id, rnd, match, pred_val)
            r1_pts = float(info.get("r1_pts", 0) or 0)
            r2_pts = float(info.get("r2_pts", 0) or 0)
            r3_pts = float(info.get("r3_pts", 0) or 0)
            await conn.execute("""
                INSERT INTO points_cache (emp_id, r1_pts, r2_pts, r3_pts, total)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (emp_id) DO UPDATE SET
                    r1_pts=EXCLUDED.r1_pts, r2_pts=EXCLUDED.r2_pts,
                    r3_pts=EXCLUDED.r3_pts, total=EXCLUDED.total""",
                emp_id, r1_pts, r2_pts, r3_pts, r1_pts+r2_pts+r3_pts)
        # Insert matches
        for match, result in data["matches"]["R1"]:
            await conn.execute(
                "INSERT INTO matches (round, match_name, result, status, played_at) VALUES ($1,$2,$3,'done',$4) ON CONFLICT DO NOTHING",
                1, match, result, "2026-06-01")
        for match, result in data["matches"]["R2"]:
            await conn.execute(
                "INSERT INTO matches (round, match_name, result, status, played_at) VALUES ($1,$2,$3,'done',$4) ON CONFLICT DO NOTHING",
                2, match, result, "2026-06-15")
        for match, result in data["matches"]["R3_scored"]:
            await conn.execute(
                "INSERT INTO matches (round, match_name, result, status, played_at) VALUES ($1,$2,$3,'done',$4) ON CONFLICT DO NOTHING",
                3, match, result, "2026-06-25")
        for match in data["matches"]["R3_pending"]:
            await conn.execute(
                "INSERT INTO matches (round, match_name, status) VALUES ($1,$2,'pending') ON CONFLICT DO NOTHING",
                3, match)
        print(f"Seeded {len(data['participants'])} participants.")

# ── Points helpers ────────────────────────────────────────
def check_pred(prediction: str, result: str) -> bool:
    p = prediction.lower()
    r = result.lower()
    if "draw" in r or "تعادل" in r:
        return "draw" in p or "تعادل" in p
    team = r.replace(" win","").replace(" wins","").strip()
    return team in p

async def recalc_points(conn, emp_id: str):
    preds = await conn.fetch("SELECT round, match_name, prediction FROM predictions WHERE emp_id=$1", emp_id)
    results_rows = await conn.fetch("SELECT round, match_name, result FROM matches WHERE status='done' AND result IS NOT NULL")
    results = {(r["round"], r["match_name"]): r["result"] for r in results_rows}
    pts = {1: 0.0, 2: 0.0, 3: 0.0}
    for pr in preds:
        key = (pr["round"], pr["match_name"])
        if key in results and pr["prediction"]:
            if check_pred(pr["prediction"], results[key]):
                pts[pr["round"]] += 3.0
    await conn.execute("""
        INSERT INTO points_cache (emp_id, r1_pts, r2_pts, r3_pts, total)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (emp_id) DO UPDATE SET
            r1_pts=EXCLUDED.r1_pts, r2_pts=EXCLUDED.r2_pts,
            r3_pts=EXCLUDED.r3_pts, total=EXCLUDED.total""",
        emp_id, pts[1], pts[2], pts[3], pts[1]+pts[2]+pts[3])

# ── PUBLIC ENDPOINTS ──────────────────────────────────────
@app.get("/api/stats")
async def get_stats():
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM participants")
        r1 = await conn.fetchval("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1")
        r2 = await conn.fetchval("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2")
        r3 = await conn.fetchval("SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3")
        all3 = await conn.fetchval("""SELECT COUNT(*) FROM (
            SELECT emp_id FROM predictions GROUP BY emp_id HAVING COUNT(DISTINCT round)=3) x""")
        top = await conn.fetchrow("SELECT emp_id, r1_pts, r2_pts, r3_pts, total FROM points_cache ORDER BY total DESC LIMIT 1")
        avg = await conn.fetchrow("SELECT AVG(total), AVG(r1_pts), AVG(r2_pts), AVG(r3_pts) FROM points_cache")
        mn = await conn.fetchval("SELECT MIN(total) FROM points_cache")
        leader_name = ""
        if top:
            p = await conn.fetchrow("SELECT name FROM participants WHERE emp_id=$1", top["emp_id"])
            leader_name = p["name"] if p else ""
        pending = await conn.fetchval("SELECT COUNT(*) FROM matches WHERE status='pending'")
        done = await conn.fetchval("SELECT COUNT(*) FROM matches WHERE status='done'")

        combos = {}
        combo_queries = {
            "r1only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=1 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r2only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=2 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r3only": "SELECT COUNT(DISTINCT emp_id) FROM predictions WHERE round=3 AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1) AND emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)",
            "r1r2": "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=2 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=3)",
            "r1r3": "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=1 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=2)",
            "r2r3": "SELECT COUNT(DISTINCT p1.emp_id) FROM predictions p1 JOIN predictions p2 ON p1.emp_id=p2.emp_id WHERE p1.round=2 AND p2.round=3 AND p1.emp_id NOT IN (SELECT emp_id FROM predictions WHERE round=1)",
        }
        for k, q in combo_queries.items():
            combos[k] = await conn.fetchval(q)
        combos["all3"] = all3

        totals = [r["total"] for r in await conn.fetch("SELECT total FROM points_cache")]
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
            "leader": {"name": leader_name,
                       "total": float(top["total"]) if top else 0,
                       "r1": float(top["r1_pts"]) if top else 0,
                       "r2": float(top["r2_pts"]) if top else 0,
                       "r3": float(top["r3_pts"]) if top else 0},
            "avg_total": round(float(avg[0] or 0), 1),
            "avg_r1": round(float(avg[1] or 0), 1),
            "avg_r2": round(float(avg[2] or 0), 1),
            "avg_r3": round(float(avg[3] or 0), 1),
            "min_total": float(mn or 0),
            "matches_done": done,
            "matches_pending": pending,
            "combos": combos,
            "dist_labels": bin_labels,
            "dist_values": dist,
        }

@app.get("/api/leaderboard")
async def get_leaderboard(page: int = 1, per_page: int = 20, search: str = "", round_filter: str = "all"):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT pc.emp_id, p.name, pc.r1_pts, pc.r2_pts, pc.r3_pts, pc.total, p.rounds
            FROM points_cache pc
            JOIN participants p ON pc.emp_id=p.emp_id
            ORDER BY pc.total DESC, p.name ASC""")
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
                "r1": float(r["r1_pts"]), "r2": float(r["r2_pts"]), "r3": float(r["r3_pts"]),
                "total": float(r["total"]),
                "p1": 1 in rounds, "p2": 2 in rounds, "p3": 3 in rounds,
            })
        total_count = len(results)
        start = (page-1)*per_page
        return {"data": results[start:start+per_page], "total": total_count, "page": page, "per_page": per_page}

@app.get("/api/matches")
async def get_matches():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM matches ORDER BY round, id")
        return [dict(r) for r in rows]

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
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM matches WHERE round=$1 AND match_name=$2",
                                       payload.round, payload.match_name)
        if not existing:
            raise HTTPException(404, f"Match not found: R{payload.round} {payload.match_name}")
        await conn.execute("""UPDATE matches SET result=$1, status='done', played_at=$2
                              WHERE round=$3 AND match_name=$4""",
                           payload.result, datetime.utcnow().isoformat(), payload.round, payload.match_name)
        participants = await conn.fetch("SELECT DISTINCT emp_id FROM predictions WHERE round=$1", payload.round)
        for p in participants:
            await recalc_points(conn, p["emp_id"])
    await broadcaster.broadcast({"type": "update", "match": payload.match_name,
                                  "result": payload.result, "round": payload.round})
    return {"status": "ok", "match": payload.match_name, "result": payload.result,
            "updated": len(participants)}

@app.post("/api/admin/result/batch")
async def submit_results_batch(results: list[MatchResult], _=Depends(require_admin)):
    updated = []
    async with pool.acquire() as conn:
        for payload in results:
            existing = await conn.fetchrow("SELECT id FROM matches WHERE round=$1 AND match_name=$2",
                                           payload.round, payload.match_name)
            if not existing:
                continue
            await conn.execute("""UPDATE matches SET result=$1, status='done', played_at=$2
                                  WHERE round=$3 AND match_name=$4""",
                               payload.result, datetime.utcnow().isoformat(), payload.round, payload.match_name)
            updated.append(payload.match_name)
        all_ids = await conn.fetch("SELECT emp_id FROM participants")
        for p in all_ids:
            await recalc_points(conn, p["emp_id"])
    await broadcaster.broadcast({"type": "update", "updated": updated})
    return {"status": "ok", "updated": updated}

@app.get("/api/admin/matches/pending")
async def get_pending(_=Depends(require_admin)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM matches WHERE status='pending' ORDER BY round, id")
        return [dict(r) for r in rows]

@app.get("/api/admin/participant/{emp_id}")
async def get_participant_detail(emp_id: str, _=Depends(require_admin)):
    async with pool.acquire() as conn:
        p = await conn.fetchrow("SELECT * FROM participants WHERE emp_id=$1", emp_id)
        if not p:
            raise HTTPException(404, "Participant not found")
        preds = await conn.fetch("SELECT round, match_name, prediction FROM predictions WHERE emp_id=$1 ORDER BY round", emp_id)
        pts = await conn.fetchrow("SELECT * FROM points_cache WHERE emp_id=$1", emp_id)
        match_results_rows = await conn.fetch("SELECT round, match_name, result FROM matches WHERE status='done'")
        match_results = {(m["round"], m["match_name"]): m["result"] for m in match_results_rows}
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
            "r1_pts": float(pts["r1_pts"]) if pts else 0,
            "r2_pts": float(pts["r2_pts"]) if pts else 0,
            "r3_pts": float(pts["r3_pts"]) if pts else 0,
            "total": float(pts["total"]) if pts else 0,
            "predictions": pred_detail,
        }

@app.post("/api/admin/reset-and-reseed")
async def reset_reseed(_=Depends(require_admin)):
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE points_cache, predictions, matches, participants RESTART IDENTITY CASCADE")
    await seed_db()
    return {"status": "ok", "message": "Database reset and reseeded successfully"}

# ── Serve frontend ────────────────────────────────────────
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
