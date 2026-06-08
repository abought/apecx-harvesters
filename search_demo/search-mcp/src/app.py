"""
APECx search and discovery MCP server.

For development, run:
    uv run search-mcp [--port PORT]
"""
import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP

from apecx_harvesters.loaders import get_query_schema
from gsearch import api as gsearch_api
from gsearch import parse as gsearch_parse

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

_CONTENT_DIR = Path(__file__).parent / "content"


class _ClientIPMiddleware:
    """Pure ASGI middleware that extracts the client IP into a ContextVar.

    Uses X-Forwarded-For when present. AWS ALB appends the real client IP as the rightmost
    entry, so we take the last value. Without XFF, falls back to the direct connection host.
    """

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            headers = {k.lower(): v for k, v in scope.get("headers", [])}
            xff = headers.get(b"x-forwarded-for", b"").decode()
            if xff:
                # Rightmost entry is set by the last trusted proxy (AWS ALB).
                ip = xff.split(",")[-1].strip()
            else:
                client = scope.get("client")
                ip = client[0] if client else "unknown"
            token = gsearch_api.client_ip_var.set(ip)
            try:
                await self._app(scope, receive, send)
            finally:
                gsearch_api.client_ip_var.reset(token)
        else:
            await self._app(scope, receive, send)


@dataclass
class Dataset:
    uuid: str
    name: str
    description: str
    schema: dict


INDICES = [
    Dataset(
        'e74bf12a-d0dd-4d19-a965-03f4936db851',
        'APECx Literature',
        'Biomedical literature and structure data from PDB, PubMed, and EMDB',
        get_query_schema()
    )
]

_INDEX_BY_ID: dict[str, Dataset] = {ds.uuid: ds for ds in INDICES}


mcp = FastMCP(
    "APECx search and discovery tool",
    json_response=True,
    instructions=(_CONTENT_DIR / "instructions.md").read_text(),
)


@mcp.resource("references://gsearch/query-syntax")
async def query_syntax() -> str:
    return (_CONTENT_DIR / "search-syntax.md").read_text()


@mcp.tool(description="List available search indices, with a brief description of the data in each")
async def list_indices() -> list[dict]:
    return [{"uuid": ds.uuid, "name": ds.name, "description": ds.description} for ds in INDICES]


@mcp.tool(description=(
    "Get the schema (field mappings) for a specified `index_id`. "
    "Use this to understand available fields and construct query payloads for `search`. "
    "Consult `references://gsearch/query-syntax` for query syntax."
))
async def get_schema(index_id: str) -> dict:
    ds = _INDEX_BY_ID.get(index_id)
    if ds is None:
        raise ValueError(f"Unknown index_id: {index_id!r}")
    return ds.schema


@mcp.tool(description=(
    "Search a Globus Search index. "
    "`payload` is a Globus Search JSON query (see `references://gsearch/query-syntax`). "
    "`include_fields` optionally restricts which top-level content fields are returned, reducing token usage."
))
async def search(
    index_id: str,
    payload: dict,
    include_fields: list[str] | None = None,
) -> dict:
    if index_id not in _INDEX_BY_ID:
        raise ValueError(f"Unknown index_id: {index_id!r}")

    ip = gsearch_api.client_ip_var.get()
    log.info("search index=%s ip=%s include_fields=%s", index_id, ip, include_fields)

    data = await gsearch_api.search(index_id, payload)

    total = int(data.get("total", 0))
    count = int(data.get("count", 0))
    log.info("search complete index=%s ip=%s total=%d count=%d", index_id, ip, total, count)

    records = []
    for gmeta_item in data.get("gmeta", []):
        content = gsearch_parse.get_entry(gmeta_item)
        if content is None:
            continue
        if include_fields is not None:
            content = gsearch_parse.filter_fields(content, include_fields)
        records.append(content)

    return {
        "total": total,
        "count": count,
        "has_next_page": data.get("has_next_page", False),
        "records": records,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    starlette_app = mcp.streamable_http_app()
    asgi_app = _ClientIPMiddleware(starlette_app)
    uvicorn.run(asgi_app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()