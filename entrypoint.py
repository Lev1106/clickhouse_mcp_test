import os
import re
import json
import asyncio
from typing import Any, Dict

from fastapi import FastAPI, Request, HTTPException
from sse_starlette.sse import EventSourceResponse
import clickhouse_connect

# ---------- ENV ----------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY")  # если хочешь, добавим проверку ниже

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

# Очередь исходящих событий для всех подключений.
# Для простоты одна глобальная. Этого достаточно для одного коннектора.
outgoing: asyncio.Queue = asyncio.Queue()

# Опциональная простая проверка ключа (и в GET, и в POST)
def ensure_auth(request: Request):
    if API_KEY:
        q = request.query_params.get("api_key")
        h = request.headers.get("X-API-Key")
        if q != API_KEY and h != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid API key")

# MCP: описание нашего единственного инструмента
QUERY_TOOL = {
    "name": "query",
    "description": "Run read-only SELECT SQL on ClickHouse",
    "inputSchema": {  # camelCase как любят клиенты
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
        "additionalProperties": False,
    },
}

# Сборка «результата» MCP в ожидаемый формат:
def mcp_result_content_json(payload: Any) -> Dict[str, Any]:
    # content должен быть списком блоков; используем блок типа json
    return {"content": [{"type": "json", "json": payload}]}

# ---------- SSE GET: стримим ответы ----------
@app.get("/sse")
async def sse_stream(request: Request):
    ensure_auth(request)

    async def gen():
        # MCP ожидает, что сначала мы ответим на initialize (но это будет через POST/response).
        # Здесь просто держим стрим и отправляем, что положили в очередь.
        # Чтобы соединение не простаивало слишком долго, можно слать heartbeat.
        heartbeat = 0
        while True:
            try:
                item = await asyncio.wait_for(outgoing.get(), timeout=20.0)
                yield {"event": "message", "data": json.dumps(item)}
            except asyncio.TimeoutError:
                # heartbeat, чтобы прокси не обрывали соединение
                heartbeat += 1
                yield {"event": "message", "data": json.dumps({"jsonrpc": "2.0", "method": "heartbeat", "params": {"n": heartbeat}})}

    return EventSourceResponse(gen())

# ---------- SSE POST: принимаем JSON-RPC запросы ----------
@app.post("/sse")
async def sse_post(request: Request):
    ensure_auth(request)
    body = await request.json()

    # Клиент может прислать массив батчем; поддержим оба варианта
    if isinstance(body, list):
        for msg in body:
            await handle_rpc(msg)
    else:
        await handle_rpc(body)

    return {"status": "ok"}

# ---------- Обработчик JSON-RPC ----------
async def handle_rpc(msg: Dict[str, Any]):
    """
    Поддерживаем два метода:
    - "initialize"  -> ответ с capabilities и списком tools
    - "tools/call"  -> выполнить наш tool 'query'
    Всё в стиле JSON-RPC 2.0: {"jsonrpc":"2.0","id":...,"method":"...","params":{...}}
    Ответ: {"jsonrpc":"2.0","id":...,"result":{...}} либо error.
    """
    jsonrpc = msg.get("jsonrpc", "2.0")
    method = msg.get("method")
    _id = msg.get("id")

    # initialize
    if method == "initialize":
        result = {
            # Версия протокола произвольная строка; важнее, чтобы была jsonrpc=2.0
            "protocolVersion": "2025-01-01",
            "capabilities": {
                "tools": {}  # говорим, что поддерживаем инструменты
            },
            "tools": [QUERY_TOOL],
        }
        await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        return

    # tools/call
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        arguments = params.get("arguments") or {}

        if name != "query":
            # неизвестный инструмент
            await outgoing.put({
                "jsonrpc": jsonrpc,
                "id": _id,
                "error": {"code": -32601, "message": f"Unknown tool: {name}"}
            })
            return

        sql = arguments.get("sql", "")
        try:
            validate_sql(sql)
            res = client.query(sql, settings={"readonly": 1, "max_execution_time": 8, "result_overflow_mode": "throw"})
            rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
            result = {
                "toolName": "query",
                # MCP рекомендует "content" как список блоков
                **mcp_result_content_json({"rows": rows, "count": len(rows)}),
            }
            await outgoing.put({"jsonrpc": jsonrpc, "id": _id, "result": result})
        except Exception as e:
            await outgoing.put({
                "jsonrpc": jsonrpc,
                "id": _id,
                "error": {"code": -32000, "message": f"{type(e).__name__}: {e}"}
            })
        return

    # неизвестный метод
    await outgoing.put({
        "jsonrpc": jsonrpc,
        "id": _id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"},
    })
