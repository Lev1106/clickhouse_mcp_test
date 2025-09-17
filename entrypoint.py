import os
import re
import json
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY", "supersecret")

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY
)

FORBIDDEN = (
    "insert", "alter", "drop", "truncate", "optimize",
    "attach", "rename", "create", "delete", "system", "grant", "revoke"
)
SELECT_RE = re.compile(r"^\s*select\b", re.IGNORECASE)

def validate_sql(sql: str):
    norm = sql.strip().lower()
    if not SELECT_RE.match(norm):
        raise ValueError("Only SELECT statements allowed")
    for kw in FORBIDDEN:
        if kw in norm:
            raise ValueError(f"Forbidden keyword: {kw}")

app = FastAPI()

@app.get("/sse")
async def sse_endpoint(request: Request):
    async def event_generator():
        # Handshake: отправляем список tools
        tools_list = {
            "type": "tools/list",
            "tools": [
                {
                    "name": "query",
                    "description": "Run read-only SELECT SQL on ClickHouse",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "sql": {"type": "string"}
                        },
                        "required": ["sql"]
                    }
                }
            ]
        }
        yield {"event": "message", "data": json.dumps(tools_list)}

        async for body in request.stream():
            try:
                event = json.loads(body.decode())
                if event.get("type") == "tools/call" and event["name"] == "query":
                    sql = event["arguments"]["sql"]
                    validate_sql(sql)
                    result = client.query(sql, settings={"readonly": 1, "max_execution_time": 8})
                    rows = [dict(zip(result.column_names, r)) for r in result.result_rows]
                    response = {
                        "type": "tools/response",
                        "name": "query",
                        "content": {"rows": rows, "count": len(rows)},
                        "call_id": event["call_id"]
                    }
                    yield {"event": "message", "data": json.dumps(response)}
            except Exception as e:
                err = {
                    "type": "tools/response",
                    "name": "query",
                    "error": str(e)
                }
                yield {"event": "message", "data": json.dumps(err)}

    return EventSourceResponse(event_generator())

