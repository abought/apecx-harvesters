# APECx-Harvesters

Tools for scraping scientific APIs: search, retrieve, and parse to a standard datacite schema + service-specific extensions. This system is designed to create a single uniform search service that aggregates data across repositories.

https://support.datacite.org/docs/datacite-xml-to-json-mapping

## Commands 
* Test: `uv run pytest`
* Lint: `uv run ruff check`
* Types: `uv run pyright src/`

## Coding style
* Comments should be concise. Do not provide usage examples or explain the algorithm unless it is unintuitive
* Capture discussion points / unclear items as TODO remarks, or in a document such as `design/OPEN_QUESTIONS.md`.
* Avoid creating fake data, such as a parser that emits synthetic values.

## Development Process
* Each time a new harvester is written, add a link to the API documentation and examples to `design/API_REFERENCE.md`. If you consult additional documents to understand the response schema, include those too.
  * Consult `API_REFERENCE.md` for key references before initiating a web search for the spec.

## Architecture
Every harvester will provide:

1. A data container that extends the base schema (`Datacite` class) with API-specific data fields. If a field from the target API can be represented in the core datacite schema, prefer parent level fields. 
2. A scraper (harvester) that retrieves items by ID from an appropriate API
3. Appropriate type and content validation
4. Unit tests that receive a captured mock api payload and verify output.

Each API-specific dataclass should be a subclass of the main `Datacite` schema. Domain-specific information should be encoded in a nested field. For example, the PDB harvester might generate:

```python
class PDBContainer(DataCite):
    pdb: PDBFields
```

These data containers are serializable to produce nested json. It should be possible to determine the json schema from the data container, and data should always validate against its own schema.
