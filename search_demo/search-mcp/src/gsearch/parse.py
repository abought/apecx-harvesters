# Response parsing for Globus Search results.
# Reference: https://docs.globus.org/api/search/reference/post_query/#gsearchresult


def get_entry(gmeta_item: dict) -> dict | None:
    """Return the content of the first entry in a gmeta item, or None if there are no entries."""
    entries = gmeta_item.get("entries", [])
    return entries[0]["content"] if entries else None


def filter_fields(content: dict, fields: list[str]) -> dict:
    """Return content restricted to the requested fields.

    Supports dot-notation for nested paths (e.g. "pdb.method", "titles.title").
    When a path segment resolves to a list, the remaining path is applied to each
    dict element in that list (e.g. "creators.name" → {"creators": [{"name": ...}, ...]}).
    """
    # Group sub-paths by their first segment.
    by_key: dict[str, list[str]] = {}
    for field in fields:
        head, _, tail = field.partition(".")
        by_key.setdefault(head, [])
        if tail:
            by_key[head].append(tail)

    result: dict = {}
    for key, sub_fields in by_key.items():
        if key not in content:
            continue
        value = content[key]
        if not sub_fields:
            result[key] = value
        elif isinstance(value, dict):
            result[key] = filter_fields(value, sub_fields)
        elif isinstance(value, list):
            result[key] = [
                filter_fields(item, sub_fields) if isinstance(item, dict) else item
                for item in value
            ]
        # else: primitive reached before path was exhausted — skip
    return result
