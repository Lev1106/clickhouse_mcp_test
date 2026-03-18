import os
import re
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

# =========================================================
# ENV
# =========================================================

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"

# Для локальной/черновой отладки с ChatGPT connector в режиме "Без авторизации"
ALLOW_UNAUTH_MCP = os.getenv("ALLOW_UNAUTH_MCP", "true").lower() == "true"

# На будущее, если захочешь простой header auth вне ChatGPT connector:
MCP_API_KEY = os.getenv("MCP_API_KEY")

DEFAULT_MAX_EXEC_TIME = int(os.getenv("MCP_DEFAULT_MAX_EXEC_TIME", "30"))
DEFAULT_MAX_RESULT_ROWS = int(os.getenv("MCP_DEFAULT_MAX_RESULT_ROWS", "1000"))

# В проде лучше задать свой домен, чтобы валидировать Origin.
ALLOWED_ORIGINS = {
    x.strip() for x in os.getenv(
        "MCP_ALLOWED_ORIGINS",
        "https://chatgpt.com,https://chat.openai.com"
    ).split(",") if x.strip()
}

# =========================================================
# ClickHouse client
# =========================================================

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY,
)

# =========================================================
# Helpers
# =========================================================

def to_jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            return bytes(v).decode("utf-8")
        except Exception:
            return bytes(v).hex()
    return v

def row_to_jsonable(cols: List[str], row: List[Any]) -> Dict[str, Any]:
    return {cols[i]: to_jsonable(row[i]) for i in range(len(cols))}

def rpc_result(id_: Union[int, str, None], result_obj: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "result": result_obj}

def rpc_error(id_: Union[int, str, None], code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}

def content_json(payload: Any) -> Dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False),
            }
        ]
    }

# =========================================================
# Security
# =========================================================

FORBIDDEN = (
    "insert", "alter", "drop", "truncate", "optimize", "attach",
    "rename", "create", "delete", "grant", "revoke"
)
ALLOWED_START_RE = re.compile(r"^\s*(select|show)\b", re.IGNORECASE)

USER_ALLOWED_SETTINGS = {
    "max_execution_time",
    "max_result_rows",
    "max_rows_to_read",
    "max_block_size",
    "max_threads",
}

def validate_origin(request: Request) -> None:
    origin = request.headers.get("origin")
    if origin and origin not in ALLOWED_ORIGINS:
        raise HTTPException(status_code=403, detail="Origin not allowed")

def ensure_post_auth(request: Request) -> None:
    # Для режима "Без авторизации" в ChatGPT connector
    if ALLOW_UNAUTH_MCP:
        return

    # Простейшая header auth на крайняк.
    # Для чувствительных данных в проде лучше OAuth 2.1 перед сервером / по spec.
    if not MCP_API_KEY:
        raise HTTPException(status_code=500, detail="Server auth is misconfigured")

    x_api_key = request.headers.get("X-API-Key")
    auth = request.headers.get("Authorization", "")
    bearer = auth[7:].strip() if auth.lower().startswith("bearer ") else None

    if x_api_key == MCP_API_KEY or bearer == MCP_API_KEY:
        return

    raise HTTPException(status_code=401, detail="Invalid API key")

def validate_sql(sql: str) -> None:
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed")

    norm = sql.strip().lower()
    if not ALLOWED_START_RE.match(norm):
        raise ValueError("Only SELECT or SHOW statements allowed")

    for kw in FORBIDDEN:
        if kw in norm:
            raise ValueError(f"Forbidden keyword: {kw}")

def build_settings(user_settings: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe = {k: user_settings[k] for k in (user_settings or {}) if k in USER_ALLOWED_SETTINGS}
    safe.setdefault("max_execution_time", DEFAULT_MAX_EXEC_TIME)
    safe.setdefault("max_result_rows", DEFAULT_MAX_RESULT_ROWS)
    safe.setdefault("max_threads", 2)
    safe.update({
        "readonly": 1,
        "result_overflow_mode": "throw",
    })
    return safe

# =========================================================
# MCP tool descriptors
# =========================================================

QUERY_TOOL = {
    "name": "query",
    "title": "Run ClickHouse query",
    "description": "Run a read-only SELECT or SHOW query against ClickHouse.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "settings": {
                "type": "object",
                "additionalProperties": {"type": ["string", "number", "boolean"]},
            },
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
}

SEARCH_TOOL = {
    "name": "search",
    "title": "Search schema knowledge",
    "description": "Search available database knowledge or documentation.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {"type": "string"}
        },
        "required": ["query"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
}

FETCH_TOOL = {
    "name": "fetch",
    "title": "Fetch schema document",
    "description": "Fetch a schema or documentation record by id.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "id": {"type": "string"}
        },
        "required": ["id"],
        "additionalProperties": False,
    },
    "annotations": {
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
}

TOOLS = [QUERY_TOOL, SEARCH_TOOL, FETCH_TOOL]

# =========================================================
# App
# =========================================================

app = FastAPI()

@app.get("/")
async def root():
    return {"ok": True, "service": "mcp-clickhouse", "endpoint": "/mcp"}
    
@app.get("/mcp")
async def mcp_sse(request: Request):
    print("GET /mcp headers =", dict(request.headers))

    async def event_generator():
        n = 0
        while True:
            if await request.is_disconnected():
                print("GET /mcp disconnected")
                break

            n += 1
            msg = {
                "jsonrpc": "2.0",
                "method": "ping",
                "params": {"ok": True, "n": n}
            }
            print("GET /mcp send =", msg)

            yield {
                "event": "message",
                "data": json.dumps(msg, ensure_ascii=False)
            }

            await asyncio.sleep(15)

    return EventSourceResponse(event_generator())

@app.post("/mcp")
async def mcp_post(request: Request):
    try:
        raw = await request.body()
        print("POST /mcp headers =", dict(request.headers))
        print("POST /mcp body =", raw.decode("utf-8", errors="ignore"))

        validate_origin(request)
        ensure_post_auth(request)

        body = json.loads(raw)
    except Exception as e:
        print("POST /mcp ERROR =", repr(e))
        raise

    if isinstance(body, list):
        responses = []
        for item in body:
            resp = await handle_rpc(item)
            if resp is not None:
                responses.append(resp)
        print("POST /mcp response =", responses)
        return JSONResponse(responses)

    resp = await handle_rpc(body)
    print("POST /mcp response =", resp)
    return JSONResponse(resp if resp is not None else {"jsonrpc": "2.0", "result": {}})

# =========================================================
# RPC
# =========================================================

async def handle_rpc(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    method = msg.get("method")
    req_id = msg.get("id")

    if method == "initialize":
        return rpc_result(req_id, {
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {"listChanged": False},
            },
            "serverInfo": {
                "name": "mcp-clickhouse",
                "version": "1.0.0",
            },
            "tools": TOOLS,
        })

    if method == "tools/list":
        return rpc_result(req_id, {"tools": TOOLS})

    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}

        if name == "search":
            query = (args.get("query") or "").strip()

            # Пока фиктивно. Потом можно заменить на поиск по schema/docs.
            payload = {
                "results": [
                    {
                        "id": "clickhouse-schema-overview",
                        "title": f"Schema overview for query: {query}" if query else "Schema overview",
                        "url": "https://your-company.example/clickhouse/schema"
                    }
                ]
            }
            return rpc_result(req_id, {
                "toolName": "search",
                **content_json(payload)
            })

        if name == "fetch":
            doc_id = args.get("id", "")
            payload = {
                "id": doc_id,
                "title": f"Document {doc_id}",
                "text": "Put your schema or documentation text here.",
                "url": f"https://your-company.example/docs/{doc_id}",
                "metadata": {"source": "clickhouse-mcp"}
            }
            return rpc_result(req_id, {
                "toolName": "fetch",
                **content_json(payload)
            })

        if name == "query":
            sql = args.get("sql", "")
            try:
                validate_sql(sql)
                settings = build_settings(args.get("settings"))
                res = client.query(sql, settings=settings)

                rows = [row_to_jsonable(res.column_names, r) for r in res.result_rows]
                payload = {
                    "columns": res.column_names,
                    "rows": rows,
                    "count": len(rows),
                }
                return rpc_result(req_id, {
                    "toolName": "query",
                    "structuredContent": {
                        "columns": res.column_names,
                        "count": len(rows),
                    },
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(payload, ensure_ascii=False),
                        }
                    ],
                    "_meta": {
                        "rowCount": len(rows)
                    }
                })
            except Exception as e:
                return rpc_error(req_id, -32000, f"{type(e).__name__}: {e}")

        return rpc_error(req_id, -32601, f"Unknown tool: {name}")

    return rpc_error(req_id, -32601, f"Unknown method: {method}")
