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
API_KEY = os.getenv("MCP_API_KEY")  # пусто = без проверки в POST

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
# Разрешаем только SELECT и SHOW. Блокируем всё мутирующее и мульти-стейтменты.
FORBIDDEN = (
    "insert","alter","drop","truncate","optimize","attach",
    "rename","create","delete","grant","revoke"
)  # "system" НЕ блокируем, читать можно

ALLOWED_START_RE = re.compile(r"^\s*(select|show)\b", re.IGNORECASE)

def validate_sql(sql: str):
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed")
    norm = sql.strip().lower()
    if not ALLOWED_START_RE.match(norm):
        raise ValueError("Only SELECT or SHOW statements allowed")
    for kw in FORBIDDEN:
        if kw in norm:
            raise ValueError(f"Forbidden keyword: {kw}")

# ---------- MCP ----------
app = FastAPI()

# Для SSE: session_id -> очередь исходящих JSON-RPC сообщений
sessions: dict[str, asyncio.Queue] = {}
last_session_id: Optional[str] = None

def queue_for(req: Request) -> Optional[asyncio.Queue]:
    sid = req.headers.get("Mcp-Session-Id")
    if sid and sid in sessions:
        return sessions[sid]
    if last_session_id and last_session_id in sessions:
        return sessions[last_session_id]
    return None  # стрима может не быть, отвечаем в теле POST

def ensure_post_auth(request: Request):
    if not API_KEY:
        return
    q = request.query_params.get("api_key")
    h = request.headers.get("X-API-Key")
    if q != API_KEY and h != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def content_text_json(payload: Any) -> Dict[str, Any]:
    # требование MCP UI: content = [{ type: "text", text: "<JSON string>" }]
    return {"content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}]}

def rpc_result(id_: Union[int,str], result_obj: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "result": result_obj}

def rpc_error(id_: Union[int,str], code: int, msg: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": msg}}

# ---- Определения инструментов ----
QUERY_TOOL = {
    "name": "query",
    "description": "Run read-only SELECT or SHOW on ClickHouse",
    "inputSchema": {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
        "additionalProperties": False,
    },
}

# фиктивный search, чтобы UI не выкидывал плашки и давал включать источник
SEARCH_TOOL = {
    "name": "search",
    "description": "Dummy search tool to satisfy ChatGPT UI",
    "inputSchema": {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
        "additionalProperties": False,
    },
}

# удобная кнопка «сколько строк в таблице»
COUNT_TOOL = {
    "name": "count_rows",
    "description": "Return SELECT count() from a given table (schema.table supported)",
    "inputSchema": {
        "type": "object",
        "properties": {"table": {"type": "string"}},
        "required": ["table"],
        "additionalProperties": False,
    },
}

TOOLS = [QUERY_TOOL, SEARCH_TOOL, COUNT_TOOL]

# ---------- Простые ручки для здоровья ----------
@app.get("/")
async def root():
    return {"ok": True, "service": "mcp-clickhouse"}

@app.get("/sse")
@app.get("/sse/")
async def sse_stream(request: Request):
    # GET без авторизации специально, чтобы стрим точно открывался
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

@app.post("/sse")
@app.post("/sse/")
async def sse_post(request: Request):
    ensure_post_auth(request)
    body = await request.json()
    print(f"[RPC IN] {body}")
    if isinstance(body, list):
        responses: List[Dict[str, Any]] = []
        for msg in body:
            resp = await handle_rpc(msg, request)
            if resp: responses.append(resp)
        return JSONResponse(responses)
    else:
        resp = await handle_rpc(body, request)
        return JSONResponse(resp if resp else {"jsonrpc":"2.0","result":{}})

# ---------- Основная логика RPC ----------
async def handle_rpc(msg: Dict[str, Any], request: Request) -> Optional[Dict[str, Any]]:
    jsonrpc = msg.get("jsonrpc", "2.0")
    method = msg.get("method")
    _id = msg.get("id")
    q = queue_for(request)

    # initialize → объявляем инструменты
    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mcp-clickhouse", "version": "0.4.0"},
            "tools": TOOLS,
        }
        out = rpc_result(_id, result)
        if q: await q.put(out)
        return out

    # tools/list
    if method == "tools/list":
        out = rpc_result(_id, {"tools": TOOLS})
        if q: await q.put(out)
        return out

    # tools/call
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}

        # 1) фиктивный search (один валидный результат с URL, чтобы UI не рыдал)
        if name == "search":
            dummy = {
                "results": [
                    {
                        "id": "noop",
                        "title": "ClickHouse MCP",
                        "url": f"https://{request.headers.get('host','example.com')}/"
                    }
                ]
            }
            out = rpc_result(_id, {"toolName": "search", **content_text_json(dummy)})
            if q: await q.put(out)
            return out

        # 2) count_rows
        if name == "count_rows":
            table = args.get("table", "").strip()
            if not table:
                out = rpc_error(_id, -32602, "Missing 'table'")
                if q: await q.put(out); return out
            sql = f"SELECT count() AS cnt FROM {table}"
            try:
                validate_sql(sql)  # пройдёт как SELECT
                res = client.query(sql, settings={
                    "readonly": 1, "max_execution_time": 8, "result_overflow_mode": "throw"
                })
                rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
                payload = {"rows": rows, "count": 1}
                out = rpc_result(_id, {"toolName":"count_rows", **content_text_json(payload)})
                if q: await q.put(out)
                return out
            except Exception as e:
                out = rpc_error(_id, -32000, f"{type(e).__name__}: {e}")
                if q: await q.put(out)
                return out

        # 3) query (SELECT/SHOW только)
        if name == "query":
            sql = args.get("sql", "")
            try:
                validate_sql(sql)
                res = client.query(sql, settings={
                    "readonly": 1, "max_execution_time": 8, "result_overflow_mode": "throw"
                })
                rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
                payload = {"rows": rows, "count": len(rows)}
                out = rpc_result(_id, {"toolName":"query", **content_text_json(payload)})
                if q: await q.put(out)
                return out
            except Exception as e:
                out = rpc_error(_id, -32000, f"{type(e).__name__}: {e}")
                if q: await q.put(out)
                return out

        # неизвестный инструмент
        out = rpc_error(_id, -32601, f"Unknown tool: {name}")
        if q: await q.put(out)
        return out

    # неизвестный метод
    out = rpc_error(_id, -32601, f"Unknown method: {method}")
    if q: await q.put(out)
    return out
