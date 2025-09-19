import os, re, json, uuid, asyncio
from typing import Any, Dict, Optional, Union, List
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
API_KEY = os.getenv("MCP_API_KEY")  # пусто = без проверки на POST

# ---------- ClickHouse ----------
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY,
)

# ---------- SQL guard ----------
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

# ---------- MCP ----------
app = FastAPI()

# session_id -> asyncio.Queue (для опционального SSE)
sessions: dict[str, asyncio.Queue] = {}
last_session_id: Optional[str] = None

def queue_for(req: Request) -> Optional[asyncio.Queue]:
    sid = req.headers.get("Mcp-Session-Id")
    if sid and sid in sessions:
        return sessions[sid]
    if last_session_id and last_session_id in sessions:
        return sessions[last_session_id]
    return None

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

def content_text_json(payload: Any) -> Dict[str, Any]:
    # Как в доке: content = [{type:"text", text:"<JSON STRING>"}]
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False)
            }
        ]
    }

def rpc_result(id_: Union[int,str], result_obj: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "result": result_obj}

def rpc_error(id_: Union[int,str], code: int, msg: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": msg}}

@app.get("/")
async def root():
    return {"ok": True, "service": "mcp-clickhouse"}

# Принимаем /sse и /sse/ (оба)
@app.get("/sse")
@app.get("/sse/")
async def sse_stream(request: Request):
    # GET /sse не требуем ключ, чтобы поток открыт у любого клиента
    session_id = str(uuid.uuid4())
    q: asyncio.Queue = asyncio.Queue()
    sessions[session_id] = q
    global last_session_id
    last_session_id = session_id
    print(f"[SSE] open {session_id}")

    async def gen():
        heartbeat = 0
        try:
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=20.0)
                    print(f"[RPC OUT][{session_id}] {item}")
                    yield {"event": "message", "data": json.dumps(item, ensure_ascii=False)}
                except asyncio.TimeoutError:
                    heartbeat += 1
                    yield {"event": "message", "data": json.dumps(
                        {"jsonrpc":"2.0","method":"heartbeat","params":{"n":heartbeat}}
                    )}
        finally:
            sessions.pop(session_id, None)
            print(f"[SSE] close {session_id}")

    resp = EventSourceResponse(gen())
    resp.headers["Mcp-Session-Id"] = session_id
    return resp

# Принимаем /sse и /sse/ (оба)
@app.post("/sse")
@app.post("/sse/")
async def sse_post(request: Request):
    ensure_post_auth(request)
    body = await request.json()
    print(f"[RPC IN] {body}")

    # Поддержка батча и единичного сообщения
    if isinstance(body, list):
        responses: List[Dict[str, Any]] = []
        for msg in body:
            resp = await handle_rpc(msg, request)
            if resp is not None:
                responses.append(resp)
        return JSONResponse(responses)
    else:
        resp = await handle_rpc(body, request)
        return JSONResponse(resp if resp is not None else {"jsonrpc":"2.0","result":{}})

async def handle_rpc(msg: Dict[str, Any], request: Request) -> Optional[Dict[str, Any]]:
    jsonrpc = msg.get("jsonrpc", "2.0")
    method = msg.get("method")
    _id = msg.get("id")
    q = queue_for(request)  # может быть None, если стрима нет

    # initialize
    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mcp-clickhouse", "version": "0.3.0"},
            "tools": [QUERY_TOOL],
        }
        out = rpc_result(_id, result)
        if q: await q.put(out)
        return out

    # tools/list
    if method == "tools/list":
        result = {"tools": [QUERY_TOOL]}
        out = rpc_result(_id, result)
        if q: await q.put(out)
        return out

    # tools/call
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
        if name != "query":
            out = rpc_error(_id, -32601, f"Unknown tool: {name}")
            if q: await q.put(out)
            return out
        sql = args.get("sql", "")
        try:
            validate_sql(sql)
            res = client.query(sql, settings={
                "readonly": 1,
                "max_execution_time": 8,
                "result_overflow_mode": "throw"
            })
            rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
            payload = {"rows": rows, "count": len(rows)}
            result_obj = {"toolName": "query", **content_text_json(payload)}
            out = rpc_result(_id, result_obj)
            if q: await q.put(out)
            return out
        except Exception as e:
            out = rpc_error(_id, -32000, f"{type(e).__name__}: {e}")
            if q: await q.put(out)
            return out

    # unknown
    out = rpc_error(_id, -32601, f"Unknown method: {method}")
    if q: await q.put(out)
    return out
