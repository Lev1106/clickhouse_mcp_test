import os, re, json, uuid, asyncio
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
import clickhouse_connect

# ---------- ENV ----------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY")  # оставь пустым, чтобы отключить проверку

# ---------- ClickHouse ----------
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY,
)

FORBIDDEN = ("insert","alter","drop","truncate","optimize","attach",
             "rename","create","delete","system","grant","revoke")
SELECT_RE = re.compile(r"^\s*select\b", re.IGNORECASE)

def validate_sql(sql: str):
    norm = sql.strip().lower()
    if not SELECT_RE.match(norm):
        raise ValueError("Only SELECT statements allowed")
    for kw in FORBIDDEN:
        if kw in norm:
            raise ValueError(f"Forbidden keyword: {kw}")

# ---------- MCP plumbing ----------
app = FastAPI()

# Сессии: session_id -> asyncio.Queue
sessions: dict[str, asyncio.Queue] = {}
last_session_id: Optional[str] = None

def get_queue_for_request(req: Request) -> asyncio.Queue:
    """Найти очередь по заголовку Mcp-Session-Id, иначе свалиться на последнюю активную."""
    sid = req.headers.get("Mcp-Session-Id")
    if sid and sid in sessions:
        return sessions[sid]
    if last_session_id and last_session_id in sessions:
        return sessions[last_session_id]
    # если совсем пусто — создадим временную очередь
    q = asyncio.Queue()
    return q

def ensure_post_auth(request: Request):
    if not API_KEY:
        return
    q = request.query_params.get("api_key")
    h = request.headers.get("X-API-Key")
    if q != API_KEY and h != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

QUERY_TOOL = {
    "name": "query",
    "description": "Run read-only SELECT SQL on ClickHouse",
    "inputSchema": {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
        "additionalProperties": False,
    },
}

def content_json(payload: Any) -> Dict[str, Any]:
    # MCP ожидает список блоков
    return {"content": [{"type": "json", "json": payload}]}

@app.get("/")
async def root():
    return {"ok": True, "service": "mcp-clickhouse"}

@app.get("/sse")
async def sse_stream(request: Request):
    # ВАЖНО: ключ НЕ проверяем на GET, чтобы стрим точно открылся
    session_id = str(uuid.uuid4())
    q: asyncio.Queue = asyncio.Queue()
    sessions[session_id] = q
    global last_session_id
    last_session_id = session_id
    print(f"[SSE] open session {session_id}")

    async def gen():
        heartbeat = 0
        try:
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=20.0)
                    print(f"[RPC OUT][{session_id}] {item}")
                    yield {"event": "message", "data": json.dumps(item)}
                except asyncio.TimeoutError:
                    heartbeat += 1
                    yield {"event": "message", "data": json.dumps(
                        {"jsonrpc":"2.0","method":"heartbeat","params":{"n":heartbeat}}
                    )}
        finally:
            # уборка
            sessions.pop(session_id, None)
            print(f"[SSE] close session {session_id}")

    resp = EventSourceResponse(gen())
    resp.headers["Mcp-Session-Id"] = session_id
    return resp

@app.post("/sse")
async def sse_post(request: Request):
    ensure_post_auth(request)
    body = await request.json()
    print(f"[RPC IN] {body}")

    if isinstance(body, list):
        for msg in body:
            await handle_rpc(msg, request)
    else:
        await handle_rpc(body, request)
    return {"status": "ok"}

async def handle_rpc(msg: Dict[str, Any], request: Request):
    jsonrpc = msg.get("jsonrpc", "2.0")
    method = msg.get("method")
    _id = msg.get("id")

    q = get_queue_for_request(request)

    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mcp-clickhouse", "version": "0.1.1"},
            "tools": [QUERY_TOOL],
        }
        await q.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        return

    if method == "tools/list":
        result = {"tools": [QUERY_TOOL]}
        await q.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        return

    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name != "query":
            await q.put({"jsonrpc": jsonrpc, "id": _id,
                         "error": {"code": -32601, "message": f"Unknown tool: {name}"}})
            return
        sql = arguments.get("sql", "")
        try:
            validate_sql(sql)
            res = client.query(sql, settings={
                "readonly": 1,
                "max_execution_time": 8,
                "result_overflow_mode": "throw"
            })
            rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
            payload = {"rows": rows, "count": len(rows)}
            result = {"toolName": "query", **content_json(payload)}
            await q.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        except Exception as e:
            await q.put({"jsonrpc": jsonrpc, "id": _id,
                         "error": {"code": -32000, "message": f"{type(e).__name__}: {e}"}})
        return

    await q.put({"jsonrpc": jsonrpc, "id": _id,
                 "error": {"code": -32601, "message": f"Unknown method: {method}"}})
