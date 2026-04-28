# /// script
# dependencies = [
#   "globus-sdk>=4.0",
# ]
# ///
"""Run a Globus Search POST query and write unwrapped content records as JSONL."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import globus_sdk


def _unwrap_entry(gmeta_item: dict[str, Any], entry_id: str | None) -> Any | None:
    """Return the content of the matching entry, or None if not found."""
    entries = gmeta_item.get("entries", [])
    if entry_id is not None:
        entries = [e for e in entries if e.get("entry_id") == entry_id]
    return entries[0]["content"] if entries else None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("index_id", help="Globus Search index ID")
    p.add_argument("--query", metavar="FILE", help="JSON query document file (default: stdin)")
    p.add_argument("--output", "-o", metavar="FILE", help="Output file (default: stdout)")
    p.add_argument("--entry-id", metavar="ID",
                   help="Filter to entries with this entry_id (default: first entry per subject)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if args.query:
        try:
            with open(args.query) as f:
                query_data = json.load(f)
        except FileNotFoundError:
            print(f"Error: query file not found: {args.query}", file=sys.stderr)
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: invalid JSON in query file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if sys.stdin.isatty():
            print("Error: provide a query via --query FILE or stdin pipe.", file=sys.stderr)
            sys.exit(1)
        try:
            query_data = json.load(sys.stdin)
        except json.JSONDecodeError as e:
            print(f"Error: invalid JSON on stdin: {e}", file=sys.stderr)
            sys.exit(1)

    client = globus_sdk.SearchClient()
    # Use non-paginated post_search so the query document's `limit` field is respected.
    # The paginated variant overrides `limit` with its own internal page size.
    try:
        response = client.post_search(args.index_id, query_data)
    except globus_sdk.SearchAPIError as e:
        print(f"Error: Globus Search API error ({e.http_status}): {e.message}", file=sys.stderr)
        sys.exit(1)
    except globus_sdk.GlobusAPIError as e:
        print(f"Error: API error ({e.http_status}): {e.message}", file=sys.stderr)
        sys.exit(1)
    except globus_sdk.NetworkError as e:
        print(f"Error: network error: {e}", file=sys.stderr)
        sys.exit(1)
    data = response.data

    total = int(data.get("total", 0))
    count = int(data.get("count", 0))
    print(f"Total matching records: {total}", file=sys.stderr)
    if count >= 10_000:
        print(
            f"Warning: {count} of {total} matching records returned — the 10,000 record API limit was reached. "
            "Results are incomplete; a scroll query is required to retrieve the full result set.",
            file=sys.stderr,
        )
    elif total > count:
        print(
            f"Note: {count} of {total} matching records returned. "
            "Increase `limit` in your query document to retrieve more.",
            file=sys.stderr,
        )

    out = open(args.output, "w") if args.output else sys.stdout
    try:
        for gmeta_item in data.get("gmeta", []):
            content = _unwrap_entry(gmeta_item, args.entry_id)
            if content is not None:
                out.write(json.dumps(content))
                out.write("\n")
    finally:
        if args.output:
            out.close()

    print(f"Fetched {count} records.", file=sys.stderr)


if __name__ == "__main__":
    main()