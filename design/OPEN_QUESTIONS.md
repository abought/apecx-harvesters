# Open Questions & Follow-up Items

Items that need a decision or further investigation before they can be closed.

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


## PubMed: search richness

PubMed supports many search options beyond what the current `PubMedHarvester` exposes. Expand search before unifying the search interface across harvesters. Defer interface unification until search operations are richer.
