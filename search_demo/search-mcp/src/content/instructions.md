# APECx Knowledge Discovery Service
Query biomedical data from a variety of sources. 

The recommended workflow is:

1. Find a search index that may have information relevant to the task: `list_indices`
2. Examine the schema to understand available fields: `get_schema(index_id)`
3. Perform a `search(index_id, payload, include_fields)` for relevant records, using the Globus Search query syntax and (optionally) knowledge of the data fields available.
    * Consult the resource `references://gsearch/query-syntax` if necessary
    *  Optionally, use the schema for the index to query specific fields.
