# Harvesters for the APECx search repository 

## Purpose
AI tools should be able to discover interesting data from many places with a single consistent query. This requires both curation (making sure that the data is relevant to the topic at hand) and harmonization (making sure that a query for "show me all results for this organism" will work across two APIs that use different nomenclature).

This repository achieves that goal using a set of harvesters that read data from many sources, and emit records aligned with the DataCite 4.7 Metadata schema (+domain specific extra fields for each datatype, like "PDB ID" for proteins).

## Key features
* Retrieve data from multiple sources using flexible query options. Basic parsing and harmonization
* Store data in cache, so that the data format can be changed and re-parsed without re-downloading from the source API
* Implements "good citizen" behaviors like rate limiting and batching so that the API doesn't ban you
* Async pipeline designed to stream large amounts of data efficiently 
 
## AI-ready search
As shown in `search_demo/`, we provide several helpers to let AI do interesting things with our data:
1. The full schema of all search fields is discoverable in machine-readable jsonschema format. 
2. Rigidly typed data ensures that we can trust what is in a field, and can automatically generate an up-to-date schema when needed (changes do not need to be made by hand)
3. Globus Search provides rich and well-documented query syntax, so the LLM knows how to construct a query


## Where to next?
* We should define a better list of items to go in the search index.
* Parsers/schemas should be updated to reflect expert curation knowledge about the meaning of fields
* Define `transforms` for harmonization (reaching out to external LLM, etc), and incorporate them into ingest pipelines
* Integrate into the SPHERICAL portal so that Public and Private datasets can be discovered in one place
* Improve the search functionality to work better with NIH query size limits
* Consider how or when to handle _non_-publication artifacts


## Example usage
To fetch results, convert them into Globus Search format, and upload them into a globus search index:

```bash
# Find and download items
uv run search-topic --term "eastern equine encephalitis"

# Parse downloaded items according to the newest standard, and write them to `output/<date>/chunk<n>.gz`
# This is designed to be incremental, so if you find new search results later, only the new stuff will be saved in a given `<date>/` folder. This makes search index updates faster, but if you intend to re-parse existing data and re-ingest, the only current way it to delete the `output/` folder so that it regenerates everything
uv run aggregate-gsearch

# Globus search has a 10MB-per-ingest chunk size limit. Submit all the chunked files and report results. This assumes your globus cli is installed and you have created a valid index with valid permissions
./scripts/ingest-gsearch "$GSI_UUID" output/<date>/
```
