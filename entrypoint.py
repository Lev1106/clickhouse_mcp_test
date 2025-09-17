import os
import re
import sys
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import clickhouse_connect

# ==== Настройки ====
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY", "supersecret")

# ==== Инициализация клиента ====
try:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=CLICKHOUSE_SECURE,
        verify=CLICKHOUSE_VERIFY
    )
except Exception as e:
    print(f"[BOOT ERROR] Failed to init ClickHouse client: {e}", file=sys.stderr)
    client = None  # чтобы не упасть на старте

# ==== Валидация SQL ====
FORBIDDEN = (
    "insert", "alter", "drop", "truncate", "optimize",
    "attach", "rename", "create", "delete", "system", "grant", "revoke"
)
ALLOWED_START = re.compile(r"^\s*select\b", re.IGNORECASE)

def validate_sql(sql: str):
    norm = sql.strip().lower()
    if not ALLOWED_START.match(norm):
        raise HTTPException(status_code=400, detail="Only SELECT statements allowed")
    for kw in FORBIDDEN:
        if kw in norm:
            raise HTTPException(status_code=400, detail=f"Forbidden keyword: {kw}")

# ==== FastAPI ====
app = FastAPI()

class SQLRequest(BaseModel):
    sql: str
    limit: int | None = 1000

@app.post("/query")
def run_query(req: SQLRequest, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if client is None:
        raise HTTPException(status_code=500, detail="ClickHouse client not initialized")
    validate_sql(req.sql)

    try:
        result = client.query(
            f"{req.sql} FORMAT JSONEachRow",
            settings={
                "readonly": 1,
                "max_execution_time": 8,
                "max_result_rows": req.limit or 5000,
                "result_overflow_mode": "throw"
            }
        )
    except Exception as e:
        print(f"[QUERY ERROR] SQL failed: {req.sql} | {e}", file=sys.stderr)
        raise HTTPException(status_code=400, detail=f"ClickHouse error: {e}")

    rows = result.result_rows
    print(f"[QUERY OK] rows={len(rows)} sql={req.sql[:80]}...", file=sys.stderr)
    return {"rows": rows, "count": len(rows)}

@app.get("/ping")
def ping():
    return {"status": "ok"}
