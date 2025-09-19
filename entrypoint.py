import os
import re
import json
import uuid
import asyncio
from typing import Any, Dict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from sse_starlette.sse import EventSourceResponse
import clickhouse_connect

# ---------- ENV ----------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY")  # оставь пустым, если не нужен

# ---------- ClickHouse client ----------
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY,
)

# ---------- SQL guard ----------
FORBIDDEN = (
    "insert", "alter", "drop", "truncate", "optimize",
    "attach", "rename", "create", "delete", "system", "grant", "revoke",
)
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

# Глобальная очередь исходящих JSON-RPC сообщений (ответов/нотификаций)
outgoing: asyncio.Queue = asyncio.Queue()

def ensure_auth(request: Request):
    if API_KEY:
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

def mcp_result_blocks(payload_text: str, payload_json: Any) -> Dict[str, Any]:
    # content: блоки для модели; structuredContent: машинам дружелюбный JSON
    return {
        "content": [{"type": "text", "text": payload_text}],
        "structuredContent": payload_json,
    }

# ---------- SSE GET: стримим ответы ----------
@app.get("/sse")
async def sse_stream(request: Request):
    ensure_auth(request)
    session_id = str(uuid.uuid4())

    async def gen():
        heartbeat = 0
        while True:
            try:
                item = await asyncio.wait_for(outgoing.get(), timeout=20.0)
                # небольшие логи для отладки на Render
                print(f"[RPC OUT] {item}")
                yield {"event": "message", "data": json.dumps(item)}
            except asyncio.TimeoutError:
                heartbeat += 1
                yield {"event": "message", "data": json.dumps({"jsonrpc": "2.0", "method": "heartbeat", "params": {"n": heartbeat}})}

    # кладём InitializeResult заголовком сессии (рекомендуется спекой)
    resp = EventSourceResponse(gen())
    resp.headers["Mcp-Session-Id"] = session_id
    return resp

# ---------- SSE POST: принимаем JSON-RPC запросы ----------
@app.post("/sse")
async def sse_post(request: Request):
    ensure_auth(request)
    body = await request.json()
    # лог входящих — чтобы видеть, что реально шлёт хост
    print(f"[RPC IN] {body}")

    if isinstance(body, list):
        for msg in body:
            await handle_rpc(msg)
    else:
        await handle_rpc(body)

    return {"status": "ok"}

# ---------- Обработчик JSON-RPC ----------
async def handle_rpc(msg: Dict[str, Any]):
    jsonrpc = msg.get("jsonrpc", "2.0")
    method = msg.get("method")
    _id = msg.get("id")

    # initialize: отвечаем версией и возможностями
    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {"listChanged": False},
                # можно добавить "resources": {}, "prompts": {} при желании
            },
            "serverInfo": {"name": "mcp-clickhouse", "version": "0.1.0"},
            "tools": [QUERY_TOOL],  # сразу объявим tool, чтобы не ждать list
        }
        await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        # клиент обычно сам шлёт notifications/initialized, мы его не шлём
        return

    # tools/list: отдай список инструментов
    if method == "tools/list":
        result = {"tools": [QUERY_TOOL]}
        await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        return

    # tools/call: исполняем query
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name != "query":
            await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "error": {"code": -32601, "message": f"Unknown tool: {name}"}})
            return
        sql = arguments.get("sql", "")
        try:
            validate_sql(sql)
            res = client.query(sql, settings={"readonly": 1, "max_execution_time": 8, "result_overflow_mode": "throw"})
            rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
            payload_text = f"Rows: {len(rows)}"
            payload_json = {"rows": rows, "count": len(rows)}
            result = {"toolName": "query", **mcp_result_blocks(payload_text, payload_json)}
            await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        except Exception as e:
            await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "error": {"code": -32000, "message": f"{type(e).__name__}: {e}"}})
        return

    # неизвестный метод
    await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "error": {"code": -32601, "message": f"Unknown method: {method}"}})
