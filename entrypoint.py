import os
import re
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import clickhouse_connect
from fastmcp import FastMCP

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_VERIFY = os.getenv("CLICKHOUSE_VERIFY", "false").lower() == "true"

DEFAULT_MAX_EXEC_TIME = int(os.getenv("MCP_DEFAULT_MAX_EXEC_TIME", "30"))
DEFAULT_MAX_RESULT_ROWS = int(os.getenv("MCP_DEFAULT_MAX_RESULT_ROWS", "200"))

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=CLICKHOUSE_SECURE,
    verify=CLICKHOUSE_VERIFY,
)

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

mcp = FastMCP(
    name="clickhouse-mcp",
    instructions=(
        "Use the query tool for read-only ClickHouse access. "
        "Only SELECT and SHOW are allowed."
    ),
)

@mcp.tool(
    title="Run ClickHouse query",
    annotations={
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
)
async def query(sql: str, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run a read-only SELECT or SHOW query against ClickHouse.
    """
    validate_sql(sql)
    res = client.query(sql, settings=build_settings(settings))
    rows = [row_to_jsonable(res.column_names, r) for r in res.result_rows]
    return {
        "columns": res.column_names,
        "rows": rows,
        "count": len(rows),
    }

@mcp.tool(
    title="Search schema knowledge",
    annotations={
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
)
async def search(query: str) -> Dict[str, Any]:
    """
    Minimal MCP-compatible search tool shape.
    """
    return {
        "results": [
            {
                "id": "clickhouse-schema-overview",
                "title": f"Schema overview for query: {query}" if query else "Schema overview",
                "url": "https://example.com/clickhouse/schema",
            }
        ]
    }

@mcp.tool(
    title="Fetch schema document",
    annotations={
        "readOnlyHint": True,
        "openWorldHint": False,
        "destructiveHint": False,
    },
)
async def fetch(id: str) -> Dict[str, Any]:
    """
    Minimal MCP-compatible fetch tool shape.
    """
    return {
        "id": id,
        "title": f"Document {id}",
        "text": "Put your schema or documentation text here.",
        "url": f"https://example.com/docs/{id}",
        "metadata": {"source": "clickhouse-mcp"},
    }

app = mcp.http_app(path="/mcp")
