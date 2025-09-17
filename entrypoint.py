import os
import re
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import clickhouse_connect

# ==== Настройки из env ====
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"
API_KEY = os.getenv("MCP_API_KEY", "supersecret")  # поменяй на свой

# ==== Инициализация клиента ====
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY
)

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
    # --- API-ключ ---
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    # --- SQL чек ---
    validate_sql(req.sql)
    # --- Выполнение с безопасными настройками ---
    result = client.query(
        f"{req.sql} FORMAT JSONEachRow",
        settings={
            "readonly": 1,
            "max_execution_time": 8,
            "max_result_rows": req.limit or 5000,
            "result_overflow_mode": "throw"
        }
    )
    rows = result.result_rows
    return {"rows": rows, "count": len(rows)}

@app.get("/ping")
def ping():
    return {"status": "ok"}

