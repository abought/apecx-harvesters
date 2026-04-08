# Purpose
Captures key references for each API we scrape, in case decisions need to be reviewed later.

# General notes on data formats
## Datacite (base schema)
All items that we scrape are expected to conform to a variant of the Datacite metadata schema.

We omit some fields that the datacite schema may include. As harvester needs expand, we may consider expanding our core model to include more of the datacite spec.

* Detailed property descriptions: https://datacite-metadata-schema.readthedocs.io/en/4.7/properties/
* Mapping of XML to json (our parsers assume the json conventions): https://support.datacite.org/docs/datacite-xml-to-json-mapping 

## Publication types
### Biorxiv
Prefer the DOI resolver in most cases.
* Docs (see "content detail"): https://api.biorxiv.org/
* Example to capture: https://api.biorxiv.org/details/medrxiv/10.1101/2020.09.09.20191205

### DOIs
DOIs can be queried to obtain metadata for many existing publications. We will use the DOI.org registration agency resolver to dispatch to three possible services under the hood: crossref, datacite, or openAlex. The original registry is preferred, with an aggregator as a fallback.

### Crossref
* Docs: https://www.crossref.org/documentation/retrieve-metadata/rest-api/#77000
* Example: https://api.crossref.org/works/doi/10.1128/mbio.01735-25
* Example: https://api.crossref.org/works/doi/10.1101/2020.09.09.20191205

#### Datacite
Handles DOIs registered with DataCite (datasets, software, grey literature, and other non-journal content).

* REST API docs: https://support.datacite.org/docs/api
* Single DOI endpoint: `https://api.datacite.org/dois/{doi}`
* Full JSON field reference: https://support.datacite.org/docs/datacite-xml-to-json-mapping
* Schema example types: https://schema.datacite.org/meta/kernel-4/#examples
* Example — documentation (Text): https://api.datacite.org/dois/10.14454/qdd3-ps68
* Example — dataset (Dataset): https://api.test.datacite.org/dois/10.82433/9184-DY35?publisher=true&affiliation=true
* Example — journal article (JournalArticle, relatedItems): https://api.test.datacite.org/dois/10.82433/q54d-pf76?publisher=true&affiliation=true
* Example — software (Software): https://api.datacite.org/dois/10.5281/zenodo.7635478?publisher=true&affiliation=true

#### OpenAlex
Fallback for DOIs registered with agencies other than Crossref or DataCite. OpenAlex aggregates metadata from many sources + adds its own annotations.

* API overview: https://docs.openalex.org/
* Single work by DOI: `https://api.openalex.org/works/doi:{doi}`
* Example: https://api.openalex.org/works/doi:10.1038/s41467-021-21317-x
* Works object schema: https://docs.openalex.org/api-entities/works/work-object

### PubMed
Article and knowledge repository with good coverage of older literature and US government publications. 

* E-utilities overview: https://www.ncbi.nlm.nih.gov/books/NBK25501
* EFetch parameters: https://www.ncbi.nlm.nih.gov/books/NBK25499/
* Base URL: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml`
* Example (single article, ORCIDs): https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=33594067&retmode=xml
* Example (structured abstract, keywords): https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=32672655&retmode=xml
* PubMed XML DTD reference: https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_250101.dtd


## Molecular structure databases
### PDB
* Base documentation for REST and GraphQL APIs (prefer GraphQL when possible): https://data.rcsb.org/#data-api
  * Detailed REST API: https://data.rcsb.org/redoc/index.html
* Schema
  * To understand the fields returned: https://data.rcsb.org/redoc/index.html#tag/Schema-Service
  * Additional information about fields: https://data.rcsb.org/data-attributes.html
* Search API (used for batch queries):
  * Docs: https://search.rcsb.org/index.html
  * Endpoint: `POST https://search.rcsb.org/rcsbsearch/v2/query`
  * Pagination: `request_options.paginate.start` (offset) + `rows` (page size, max 10,000)
  * Response: `total_count` (int) + `result_set` (array of `{"identifier": "4HHB", ...}`)
  * `return_type: "entry"` yields bare PDB IDs; other types yield compound IDs
  * Terminal node requires `type: "terminal"`, `service: "text"`, and `parameters`

### EMDB
The Electron Microscopy database provides structure data from electron microscopy.
* API docs: https://www.ebi.ac.uk/emdb/api/
* Single entry endpoint: `https://www.ebi.ac.uk/emdb/api/entry/{id}` (e.g. `EMD-74041`)
* EMD-74041 example: https://www.ebi.ac.uk/emdb/api/entry/EMD-74041

Batch ID discovery uses the EBI Search infrastructure (not the EMDB native search endpoint, which returns full records and has unclear pagination):
* Endpoint: `GET https://www.ebi.ac.uk/ebisearch/ws/rest/emdb`
* Params: `query` (query string), `fields=id` (return IDs only), `size` (page size), `start` (offset)
* Response: `{"hitCount": N, "entries": [{"id": "EMD-NNNNN"}, ...]}`
* EBI Search docs: https://www.ebi.ac.uk/ebisearch/documentation/rest-api

# Ignore these; may be used later
## Protein and design databases
### Protabank
unreachable as I write this, revisit

### AntiviralDB
Unclear if they have an API

## Other things scraped by MU 

VIOLIN 
BVBRC
