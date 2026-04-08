# Open Questions & Follow-up Items

Items that need a decision or further investigation before they can be closed.

## DataCite schema compliance

~~Audit all domain-specific model fields (`PDBFields`, `PubMedFields`, etc.) against the base `DataCite` schema. Identify any sub-document fields that could be promoted to parent-level DataCite fields instead. Goal is to maximise interoperability without losing source provenance.~~

Completed in session 2026-04-02. Added to base `DataCite`: `identifiers`, `alternateIdentifiers`, `contributors`, `publicationYear`, `types` (with `ResourceTypeGeneral` enum). `DataCite.new(doi=)` now populates `identifiers` instead of `relatedIdentifiers:IsIdenticalTo`. Promoted fields: PubMed PMID/PMC → `alternateIdentifiers`; PDB accession → `alternateIdentifiers`; EMDB accession → `alternateIdentifiers`; BiorXiv corresponding author → `contributors`.

Remaining deferred items (intentionally omitted until ready):
- `Subject.subjectScheme` / `valueUri` — omitted until a cross-database subject ontology is established; premature population risks schema violations
- `nameType` on Creator (Personal/Organizational) — deferred; no current consumer
- `Publisher` identifier fields — no source data available

## PDB: polymer entity harmonization

`PDBFields.polymer_entities` records the source organism and polymer type per entity, preserving the entity-organism association that the prior art lost through flattening. However, the correct harmonization strategy for multi-organism complexes (e.g. a viral antigen bound to a human antibody) is a **domain expert decision**:

- How should a structure with two organisms be represented in search? One record or two?
- Should the organism list be promoted to `DataCite.subjects` or a custom field?
- What is the canonical "organism" of a complex — the target, the host, all of them?

See `TODO` comment in `PDBFields.polymer_entities`.

## PDB: DNA-containing structure fixture

`polymer_type` is tested for `Protein` and `RNA` (via 4ZT0) but not `DNA` or `NA-hybrid`. A nucleosome (e.g. 3LZ0) or Cas9+DNA structure would cover this branch if it becomes relevant.

## IEDB harvester

The prior art (`prior_art/tools/iedb_basic_data_collector/`) harvests epitope data from `https://query-api.iedb.org/epitope_export`. The IEDB schema (epitope identity, source molecule, position, related object) does not map naturally to DataCite. Needs a design discussion before implementation:

- What goes in base `DataCite` fields vs. a nested `IEDBFields`?
- What is the canonical identifier? (Prior art used MD5 hash; IEDB IRI may be more appropriate.)

## Refactor `DataCite.new()` into parser helpers

`DataCite.new()` uses `**kwargs: Any` + `cls(**kwargs)`, which prevents pyright from narrowing the return type to `Self`. Callers in test files see `DataCite` instead of the concrete subclass (e.g. `PDBContainer`), causing spurious attribute errors on subclass-specific fields like `.pdb`.

Fix: extract the `doi`/`title`/`description` shorthand logic from `new()` into standalone helpers in `loaders/base/parser.py`, then rewrite each parser to call the concrete constructor directly (e.g. `PDBContainer(titles=[...], identifier=Identifier(...))`). This gives pyright unambiguous return types and allows `DataCite.new()` to be removed.

## PubMed: PubmedBookArticle support

PubMed search results can include book entries (`PubmedBookArticle`). These are currently retrieved from efetch but rejected at parse time with an explicit unsupported-type error. Key differences from `PubmedArticle`:

- Title is in `BookDocument/Book/BookTitle` (whole book) or `BookDocument/ArticleTitle` (chapter)
- Authors appear at both the book and section level
- No journal; publisher info is in `BookDocument/Book/Publisher`
- Dates are in `BookDocument/Book/BeginningDate` / `ContributionDate`

The `PubmedBookArticle` DTD section in the [PubMed XML DTD](https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_250101.dtd) is the reference. NBK books (NCBI Bookshelf) are the most common source.

## Query schema: inline single-reference specialist $defs

`PDBFields`, `PubMedFields`, `EMDBFields`, and `BiorXivFields` are each referenced exactly once in the query schema. Inlining them at their use site would place domain-specific filterable fields immediately adjacent to the source discriminator property, reducing the cognitive step of $ref dereferencing for an LLM building queries.

Deferred pending baseline query evaluation — establish correctness criteria before further schema tinkering.

## PubMed: search richness

PubMed supports many search options beyond what the current `PubMedHarvester` exposes. Expand search before unifying the search interface across harvesters. Defer interface unification until search operations are richer.

## PubMed: 9,999-record eSearch limit

The eSearch API rejects `retstart > 9998`, capping retrieval at 9,999 records per query. The current code respects this limit and warns when total results exceed it. For production ingest of high-volume topics (e.g. "influenza"), consider switching to EDirect, which batches internally and can retrieve arbitrary result counts. See: https://www.ncbi.nlm.nih.gov/books/NBK25499/