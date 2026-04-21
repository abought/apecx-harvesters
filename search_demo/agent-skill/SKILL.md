---
name: apecx-discovery
description: Query a harmonized biomedical search index (PubMed, PDB, EMDB, bioRxiv) to answer scientific questions about proteins, structures, and literature. Use when the user asks about biomedical data, protein structures, research articles, drug targets, or wants to explore or search the APECx dataset.
compatibility: Requires jq
allowed-tools: Bash(uv *) Bash(jq *)
---

## Purpose

This skill provides access to a harmonized repository of biomedical records spanning
PubMed articles, PDB protein structures, EMDB cryo-EM maps, and bioRxiv preprints.
Records share a common schema (DataCite 4.7) with source-specific extensions.

The index ID is: `e74bf12a-d0dd-4d19-a965-03f4936db851` (public, no authentication required)

## References

Read these before constructing a query:

- `references/query-api.md` — how to construct a Globus Search JSON query (filters, facets, boosts, sorting)
- `references/schema.json` — all fields available in this index; use this to find correct field names and types

## Available scripts

- `scripts/fetch.py` — run a query and emit results as JSONL (one record per line)

Run `uv run scripts/fetch.py --help` to see the full interface. Minimal usage:

```bash
uv run scripts/fetch.py <index_id> --query query.json
```

Or pipe a query document via stdin:

```bash
echo '{"q": "SOD1", "limit": 100}' | uv run scripts/fetch.py e74bf12a-d0dd-4d19-a965-03f4936db851
```

## Workflow

1. Consult `references/schema.json` to identify relevant fields for the question
2. Consult `references/query-api.md` to construct the JSON query payload
3. Run the query via `scripts/fetch.py`; results are emitted as JSONL to stdout
4. Pipe through `jq` to extract only the fields needed before passing to the LLM:

```bash
echo '{"q": "amyotrophic lateral sclerosis", "limit": 200}' \
  | uv run scripts/fetch.py e74bf12a-d0dd-4d19-a965-03f4936db851 \
  | jq -s '[.[] | {doi: .identifier.identifier, title: .titles[0].title, year: .publicationYear, pdb_id: .pdb?.pdb_id}]'
```

Reducing fields with `jq` before synthesis is strongly recommended — raw records are large
and most fields will not be relevant to the question at hand.

## Gotchas

- Field names in filters must match `references/schema.json` exactly — dot-notation for nested fields (e.g. `publisher.name`, `pdb.method`)
- Domain-specific fields (`pdb`, `pubmed`, `emdb`, `biorxiv`) are only present on records from that source; use `.pdb?` in jq to handle missing fields gracefully
- `creators.name` is the reliable field for author queries — Globus Search flattens nested arrays, so filtering on split `givenName`/`familyName` fields cannot guarantee they co-locate within the same creator object
- `limit` in the query document controls how many records are returned (max 10,000); the script does not add a default limit, so always specify one
