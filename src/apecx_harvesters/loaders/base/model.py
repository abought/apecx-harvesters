"""
Base DataCite schema. All specialist repositories will extend this core schema.

Simplified from DataCite 4.6; represents the common metadata fields expected
across all harvested datasets. See CLAUDE.md for the full JSON schema specification.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, ClassVar, Optional, Self

from pydantic import BaseModel, ConfigDict, Field


class DateType(str, Enum):
    """Controlled vocabulary for date types.  Enumerated as dateType in DataCite Metadata Schema 4.x."""
    Accepted = "Accepted"
    Available = "Available"
    Copyrighted = "Copyrighted"
    Collected = "Collected"
    Created = "Created"
    Issued = "Issued"
    Submitted = "Submitted"
    Updated = "Updated"
    Valid = "Valid"
    Withdrawn = "Withdrawn"
    Other = "Other"


class TitleType(str, Enum):
    """Controlled vocabulary for title types.  Enumerated as titleType in DataCite Metadata Schema 4.x."""
    AlternativeTitle = "AlternativeTitle"
    Subtitle = "Subtitle"
    TranslatedTitle = "TranslatedTitle"
    Other = "Other"


class DescriptionType(str, Enum):
    """Controlled vocabulary for description types.  Enumerated as descriptionType in DataCite Metadata Schema 4.x."""
    Abstract = "Abstract"
    Methods = "Methods"
    SeriesInformation = "SeriesInformation"
    TableOfContents = "TableOfContents"
    TechnicalInfo = "TechnicalInfo"
    Other = "Other"


class RelatedIdentifierType(str, Enum):
    """Controlled vocabulary for identifier types.  Enumerated as relatedIdentifierType in DataCite Metadata Schema 4.x."""
    ARK = "ARK"
    arXiv = "arXiv"
    bibcode = "bibcode"
    DOI = "DOI"
    EAN13 = "EAN13"
    EISSN = "EISSN"
    Handle = "Handle"
    IGSN = "IGSN"
    ISBN = "ISBN"
    ISSN = "ISSN"
    ISTC = "ISTC"
    LISSN = "LISSN"
    LSID = "LSID"
    PMID = "PMID"
    PURL = "PURL"
    UPC = "UPC"
    URN = "URN"
    URL = "URL"
    w3id = "w3id"


class RelationType(str, Enum):
    """Controlled vocabulary for relation types.  Enumerated as relationType in DataCite Metadata Schema 4.x."""
    IsCitedBy = "IsCitedBy"
    Cites = "Cites"
    IsSupplementTo = "IsSupplementTo"
    IsSupplementedBy = "IsSupplementedBy"
    IsContinuedBy = "IsContinuedBy"
    Continues = "Continues"
    IsDescribedBy = "IsDescribedBy"
    Describes = "Describes"
    HasMetadata = "HasMetadata"
    IsMetadataFor = "IsMetadataFor"
    HasVersion = "HasVersion"
    IsVersionOf = "IsVersionOf"
    IsNewVersionOf = "IsNewVersionOf"
    IsPreviousVersionOf = "IsPreviousVersionOf"
    IsPartOf = "IsPartOf"
    HasPart = "HasPart"
    IsPublishedIn = "IsPublishedIn"
    IsReferencedBy = "IsReferencedBy"
    References = "References"
    IsDocumentedBy = "IsDocumentedBy"
    Documents = "Documents"
    IsCompiledBy = "IsCompiledBy"
    Compiles = "Compiles"
    IsVariantFormOf = "IsVariantFormOf"
    IsOriginalFormOf = "IsOriginalFormOf"
    IsIdenticalTo = "IsIdenticalTo"
    IsReviewedBy = "IsReviewedBy"
    Reviews = "Reviews"
    IsDerivedFrom = "IsDerivedFrom"
    IsSourceOf = "IsSourceOf"
    IsRequiredBy = "IsRequiredBy"
    Requires = "Requires"
    IsObsoletedBy = "IsObsoletedBy"
    Obsoletes = "Obsoletes"
    IsCollectedBy = "IsCollectedBy"
    Collects = "Collects"


class ResourceTypeGeneral(str, Enum):
    """Resource type controlled vocabulary.  Enumerated as resourceTypeGeneral in DataCite Metadata Schema 4.x."""
    Audiovisual = "Audiovisual"
    Book = "Book"
    BookChapter = "BookChapter"
    Collection = "Collection"
    ComputationalNotebook = "ComputationalNotebook"
    ConferencePaper = "ConferencePaper"
    ConferenceProceeding = "ConferenceProceeding"
    DataPaper = "DataPaper"
    Dataset = "Dataset"
    Dissertation = "Dissertation"
    Event = "Event"
    Image = "Image"
    Instrument = "Instrument"
    InteractiveResource = "InteractiveResource"
    Journal = "Journal"
    JournalArticle = "JournalArticle"
    Model = "Model"
    OutputManagementPlan = "OutputManagementPlan"
    PeerReview = "PeerReview"
    PhysicalObject = "PhysicalObject"
    Preprint = "Preprint"
    Report = "Report"
    Service = "Service"
    Software = "Software"
    Sound = "Sound"
    Standard = "Standard"
    StudyRegistration = "StudyRegistration"
    Text = "Text"
    Workflow = "Workflow"
    Other = "Other"


class Affiliation(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: Annotated[str, Field(
        title="Affiliation",
        description="Institution / employer for this author",
    )]


class NameIdentifier(BaseModel):
    """A persistent identifier for a creator, e.g. an ORCID or ISNI."""
    model_config = ConfigDict(strict=True, extra="forbid")

    nameIdentifier: Annotated[str, Field(
        title="Name Identifier",
        description="The identifier value, e.g. '0000-0002-9072-1017'",
    )]
    nameIdentifierScheme: Annotated[str, Field(
        title="Name Identifier Scheme",
        description="The identifier scheme, e.g. 'ORCID', 'ISNI', 'ROR'",
    )]
    schemeUri: Annotated[Optional[str], Field(
        title="Scheme URI",
        description="The URI of the identifier scheme, e.g. 'https://orcid.org'",
    )] = None


class Creator(BaseModel):
    """
    An author or contributor.

    Not all source APIs provide structured name data, so all name fields are
    optional.  At least one of `name` or the `givenName` / `familyName`
    pair should be populated by the harvester.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    givenName: Annotated[Optional[str], Field(title="Given Name")] = None
    familyName: Annotated[Optional[str], Field(title="Family name")] = None
    name: Optional[str] = None
    affiliation: Optional[Affiliation] = None
    nameIdentifiers: list[NameIdentifier] = Field(default_factory=list)


class ContributorType(str, Enum):
    """Controlled vocabulary for contributor roles.  Enumerated as contributorType in DataCite Metadata Schema 4.x."""
    ContactPerson = "ContactPerson"
    DataCollector = "DataCollector"
    DataCurator = "DataCurator"
    DataManager = "DataManager"
    Distributor = "Distributor"
    Editor = "Editor"
    HostingInstitution = "HostingInstitution"
    Other = "Other"
    Producer = "Producer"
    ProjectLeader = "ProjectLeader"
    ProjectManager = "ProjectManager"
    ProjectMember = "ProjectMember"
    RegistrationAgency = "RegistrationAgency"
    RegistrationAuthority = "RegistrationAuthority"
    RelatedPerson = "RelatedPerson"
    Researcher = "Researcher"
    ResearchGroup = "ResearchGroup"
    RightsHolder = "RightsHolder"
    Sponsor = "Sponsor"
    Supervisor = "Supervisor"
    Translator = "Translator"
    WorkPackageLeader = "WorkPackageLeader"


class Contributor(BaseModel):
    """
    A contributor to the dataset other than the primary creators.

    Uses the same name-field structure as Creator, plus a required contributorType.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    contributorType: ContributorType
    givenName: Annotated[Optional[str], Field(title="Given Name")] = None
    familyName: Annotated[Optional[str], Field(title="Family Name")] = None
    name: Optional[str] = None
    affiliation: Optional[Affiliation] = None
    nameIdentifiers: list[NameIdentifier] = Field(default_factory=list)


class Publisher(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: Annotated[str, Field(
        title="Organization",
        description="What organization produced the dataset",
    )]


class Date(BaseModel):
    """A date associated with the dataset, stored as an ISO 8601 date-time string."""
    model_config = ConfigDict(strict=True, extra="forbid")

    date: str = Field(json_schema_extra={"format": "date-time"})
    dateType: DateType


class Title(BaseModel):
    """A title for the dataset.  Absence of `titleType` indicates the main title."""
    model_config = ConfigDict(strict=True, extra="forbid")

    title: Annotated[str, Field(title="Title", description="The title text")]
    titleType: Annotated[Optional[TitleType], Field(
        title="Title Type",
        description="Omit for the primary title; set for subtitles, translations, etc.",
    )] = None


class Description(BaseModel):
    """A description of the dataset."""
    model_config = ConfigDict(strict=True, extra="forbid")

    description: Annotated[str, Field(title="Description")]
    descriptionType: Annotated[DescriptionType, Field(
        title="Description Type",
        description="Enumerated value",
    )]


class Rights(BaseModel):
    """A rights statement or license for the dataset."""
    model_config = ConfigDict(strict=True, extra="forbid")

    rights: Annotated[str, Field(
        title="Rights",
        description="A rights or license statement, e.g. 'Creative Commons Attribution 4.0'",
    )]
    rightsUri: Annotated[Optional[str], Field(
        title="Rights URI",
        description="URI of the license, e.g. 'https://creativecommons.org/licenses/by/4.0/'",
    )] = None
    rightsIdentifier: Annotated[Optional[str], Field(
        title="Rights Identifier",
        description="A short standardised identifier, e.g. 'cc-by-4.0'",
    )] = None


class FundingReference(BaseModel):
    """A funding source for the dataset."""
    model_config = ConfigDict(strict=True, extra="forbid")

    funderName: Annotated[str, Field(
        title="Funder Name",
    )]
    awardNumber: Annotated[Optional[str], Field(
        title="Award Number",
        description="The grant or award number",
    )] = None
    awardTitle: Annotated[Optional[str], Field(
        title="Award Title",
        description="The title of the grant or award",
    )] = None


class Subject(BaseModel):
    """
    A subject, keyword, or classification code describing the dataset.

    This is a simplified subset of the DataCite subject property — only the
    free-text `subject` value is captured.  The full spec also supports
    `subjectScheme`, `schemeUri`, and `valueUri` for structured ontology terms,
    but most sources provide plain keywords rather than formal ontology entries.
    Harvesters should map keyword lists (e.g. PDB `struct_keywords`) to this field,
        or harmonize records using unspecified custom logic.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    subject: Annotated[str, Field(title="Subject", description="Keywords")]


class RelatedIdentifier(BaseModel):
    """
    A related resource and its relationship to this dataset.

    Use when the relationship is expressible as PID-to-PID and no further
    description of the related resource is needed (e.g. linking to a related
    dataset, software repo, or preprint).  When you need to preserve
    bibliographic metadata — title, year, pages — use ``RelatedItem`` instead.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    relatedIdentifier: Annotated[str, Field(
        title="Related Identifier",
        description="e.g. a DOI or URL",
    )]
    relatedIdentifierType: Annotated[RelatedIdentifierType, Field(
        title="Identifier Type",
        description="Enumerated value",
    )]
    relationType: Annotated[RelationType, Field(
        title="Relation Type",
        description="The relationship of this dataset to the related resource",
    )]


class RelatedItemType(str, Enum):
    """Resource type of a related item.  Enumerated as relatedItemType in DataCite Metadata Schema 4.x."""
    Audiovisual = "Audiovisual"
    Book = "Book"
    BookChapter = "BookChapter"
    Collection = "Collection"
    ComputationalNotebook = "ComputationalNotebook"
    ConferencePaper = "ConferencePaper"
    ConferenceProceeding = "ConferenceProceeding"
    DataPaper = "DataPaper"
    Dataset = "Dataset"
    Dissertation = "Dissertation"
    Event = "Event"
    Image = "Image"
    Instrument = "Instrument"
    InteractiveResource = "InteractiveResource"
    Journal = "Journal"
    JournalArticle = "JournalArticle"
    Model = "Model"
    OutputManagementPlan = "OutputManagementPlan"
    PeerReview = "PeerReview"
    PhysicalObject = "PhysicalObject"
    Preprint = "Preprint"
    Report = "Report"
    Service = "Service"
    Software = "Software"
    Sound = "Sound"
    Standard = "Standard"
    StudyRegistration = "StudyRegistration"
    Text = "Text"
    Workflow = "Workflow"
    Other = "Other"


class RelatedItemIdentifier(BaseModel):
    """The persistent identifier for a related item."""
    model_config = ConfigDict(strict=True, extra="forbid")

    relatedItemIdentifier: Annotated[str, Field(
        title="Related Item Identifier",
        description="e.g. a DOI or PubMed ID",
    )]
    relatedItemIdentifierType: Annotated[RelatedIdentifierType, Field(
        title="Identifier Type",
        description="Enumerated value",
    )]


class RelatedItem(BaseModel):
    """
    A related resource with bibliographic metadata.

    Use when you need to preserve metadata about the related resource alongside
    the relationship — title, year, pages, etc.  A journal article citation is
    the canonical use case (e.g. the primary publication for a PDB structure).
    For simple PID-to-PID relationships where no further description is needed,
    use ``RelatedIdentifier`` instead.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    relationType: Annotated[RelationType, Field(
        title="Relation Type",
        description="The relationship of this dataset to the related item",
    )]
    relatedItemType: Annotated[RelatedItemType, Field(
        title="Related Item Type",
        description="Enumerated value",
    )]
    relatedItemIdentifier: Annotated[Optional[RelatedItemIdentifier], Field(
        title="Identifier",
        description="Persistent identifier, if available",
    )] = None
    titles: list[Title] = Field(default_factory=list)
    creators: list[Creator] = Field(default_factory=list)
    publicationYear: Annotated[Optional[str], Field(
        title="Publication Year",
        description="4-digit publication year",
    )] = None
    volume: Optional[str] = None
    issue: Optional[str] = None
    number: Optional[str] = None
    firstPage: Optional[str] = None
    lastPage: Optional[str] = None
    publisher: Optional[str] = None
    edition: Optional[str] = None


class Identifier(BaseModel):
    """The primary persistent identifier for this record (e.g. the DOI minted for it)."""
    model_config = ConfigDict(strict=True, extra="forbid")

    identifier: Annotated[str, Field(
        title="Identifier",
        description="The identifier value, typically a DOI",
    )]
    identifierType: Annotated[str, Field(
        title="Identifier Type",
        description="The identifier scheme, e.g. 'DOI'",
    )]


class AlternateIdentifier(BaseModel):
    """An alternative identifier for the same resource (e.g. a database accession or PMID)."""
    model_config = ConfigDict(strict=True, extra="forbid")

    alternateIdentifier: Annotated[str, Field(
        title="Alternate Identifier",
        description="An identifier for the same resource using a different scheme",
    )]
    alternateIdentifierType: Annotated[str, Field(
        title="Alternate Identifier Type",
        description="The scheme of the alternate identifier, e.g. 'PDB', 'PMID', 'EMDB'",
    )]


class ResourceType(BaseModel):
    """Resource type classification for this record."""
    model_config = ConfigDict(strict=True, extra="forbid")

    resourceTypeGeneral: Annotated[ResourceTypeGeneral, Field(
        title="Resource Type (General)",
        description="Enumerated value",
    )]
    resourceType: Annotated[Optional[str], Field(
        title="Resource Type",
        description="Free-text description of the resource type",
    )] = None


class DataCite(BaseModel):
    """
    Core metadata record harmonized with the DataCite 4.6 schema.

    Subclasses should extend this with a nested domain-specific model::

        class PDBContainer(DataCite):
            pdb: PDBFields

    Pydantic validates all fields on construction, so a `DataCite` instance
    that exists is always internally consistent.
    """
    model_config = ConfigDict(strict=True, extra="forbid")

    _schema_title: ClassVar[str] = "Search metadata"
    _schema_description: ClassVar[str] = (
        "Metadata that will be used to populate a search index. "
        "Simplified from datacite 4.6; in future we will add custom extra fields."
    )

    @classmethod
    def new(
        cls,
        *,
        title: str | None = None,
        description: str | None = None,
        doi: str | None = None,
        **kwargs: Any,
    ) -> Self:
        """
        Helper makes common datacite stuff easier to work with, eg dois.
        """
        if title is not None:
            kwargs["titles"] = [Title(title=title), *kwargs.get("titles", [])]
        if description is not None:
            kwargs["descriptions"] = [
                Description(description=description, descriptionType=DescriptionType.Abstract),
                *kwargs.get("descriptions", []),
            ]
        if doi is not None:
            kwargs["identifier"] = Identifier(identifier=doi, identifierType="DOI")
        return cls(**kwargs)

    identifier: Annotated[Optional[Identifier], Field(
        title="Identifier",
        description="The primary persistent identifier for this record, typically a DOI",
    )] = None
    creators: Annotated[list[Creator], Field(title="Authors", description="A list of authors")]
    titles: Annotated[list[Title], Field(title="Titles", description="One or more titles for this dataset")]
    publisher: Publisher
    publicationYear: Annotated[Optional[str], Field(
        title="Publication Year",
        description="4-digit year",
    )] = None
    resourceType: Annotated[Optional[ResourceType], Field(
        title="Resource Type",
        description="General and specific resource type classification",
    )] = None
    contributors: list[Contributor] = Field(default_factory=list)
    dates: list[Date] = Field(default_factory=list)
    language: Annotated[Optional[str], Field(
        title="Language",
        description="Primary language of the resource (ISO 639-1 or 639-3), e.g. 'en'",
    )] = None
    alternateIdentifiers: Annotated[list[AlternateIdentifier], Field(
        title="Alternate Identifiers",
        description="Other identifiers for the same resource (e.g. database accession codes)",
    )] = Field(default_factory=list)
    descriptions: Annotated[list[Description], Field(title="Descriptions", description="One or more descriptions of this dataset")] = Field(default_factory=list)
    formats: Annotated[list[str], Field(
        title="Formats",
        description="File formats or MIME types for this dataset, e.g. ['chemical/x-mmcif']",
    )] = Field(default_factory=list)
    version: Annotated[Optional[str], Field(
        title="Version",
        description="Version identifier for this dataset",
    )] = None
    subjects: list[Subject] = Field(default_factory=list)
    rightsList: list[Rights] = Field(default_factory=list)
    fundingReferences: list[FundingReference] = Field(default_factory=list)
    relatedIdentifiers: Annotated[list[RelatedIdentifier], Field(
        title="Related Identifiers",
        description=(
            "PID-to-PID relationships. Use when the related resource is fully "
            "identified by its persistent identifier and no further description "
            "is needed. For citation-style references where title, year, or "
            "other metadata should be preserved, use relatedItems instead."
        ),
    )] = Field(default_factory=list)
    relatedItems: Annotated[list[RelatedItem], Field(
        title="Related Items",
        description=(
            "Related resources with bibliographic metadata. Use when describing "
            "a citation where title, year, pages, etc. should be preserved "
            "alongside the relationship. For simple PID-to-PID links, use "
            "relatedIdentifiers instead."
        ),
    )] = Field(default_factory=list)

    @property
    def canonical_uri(self) -> str:
        """
        Stable, globally unique URI for this record. Defaults to `{type}:{identifier}`
        """
        prefix = type(self).__name__.removesuffix("Container").lower()
        if self.identifier:
            return f"{prefix}:{self.identifier.identifier}"
        raise ValueError(
            f"{type(self).__name__} has no primary identifier; override canonical_uri"
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize this record to a JSON-compatible dict.

        Note: `None` values are excluded per pydantic behaviors
        """
        return self.model_dump(exclude_none=True, mode="json")

    @classmethod
    def json_schema(cls) -> dict[str, Any]:
        """
        Return the JSON Schema for this model.

        Wraps Pydantic's `model_json_schema` and adds the `$schema`
        URI and document-level `title` / `description`.  Subclasses
        override `_schema_title` and `_schema_description` as class
        variables to customize those values.
        """
        schema = cls.model_json_schema()
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["title"] = cls._schema_title
        schema["description"] = cls._schema_description
        return schema
