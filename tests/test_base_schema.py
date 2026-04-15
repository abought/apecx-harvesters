"""
Unit tests for the base DataCite schema.

These tests exercise schema generation, serialisation, and Pydantic type
enforcement using directly-constructed DataCite objects.  No harvester or
API fixture is involved.
"""

from __future__ import annotations

import jsonschema
import pytest
from pydantic import ValidationError

from apecx_harvesters.loaders.base import (
    Affiliation,
    Creator,
    DataCite,
    Date,
    DateType,
    Description,
    DescriptionType,
    Identifier,
    NameIdentifier,
    Publisher,
    RelatedIdentifier,
    RelatedIdentifierType,
    RelationType,
    Title,
    TitleType,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_record() -> DataCite:
    """A valid DataCite instance with only required fields populated."""
    return DataCite.new(
        creators=[Creator(familyName="Smith", givenName="J.")],
        title="A test dataset",
        description="A one-paragraph description.",
        publisher=Publisher(name="Test Publisher"),
    )


@pytest.fixture
def full_record() -> DataCite:
    """A DataCite instance with all optional fields populated."""
    return DataCite.new(
        creators=[
            Creator(
                familyName="Smith",
                givenName="Jane",
                name="Smith, Jane",
                affiliation=Affiliation(name="Example University"),
            )
        ],
        title="A fully-populated dataset",
        description="A description with all optional fields present.",
        publisher=Publisher(name="Test Publisher"),
        dates=[
            Date(date="2024-01-15T00:00:00Z", dateType=DateType.Created),
            Date(date="2024-01-10T00:00:00Z", dateType=DateType.Submitted),
        ],
        relatedIdentifiers=[
            RelatedIdentifier(
                relatedIdentifier="10.1234/example",
                relatedIdentifierType=RelatedIdentifierType.DOI,
                relationType=RelationType.IsIdenticalTo,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Schema generation
# ---------------------------------------------------------------------------

class TestSchemaGeneration:
    def test_has_schema_uri(self):
        assert DataCite.json_schema()["$schema"] == "https://json-schema.org/draft/2020-12/schema"

    def test_title(self):
        assert DataCite.json_schema()["title"] == "Search metadata"

    def test_description(self):
        assert "search index" in DataCite.json_schema()["description"]

    def test_type_is_object(self):
        assert DataCite.json_schema()["type"] == "object"

    def test_required_fields_present(self):
        required = DataCite.json_schema()["required"]
        for field in ("creators", "titles", "publisher"):
            assert field in required

    def test_additional_properties_forbidden(self):
        assert DataCite.json_schema().get("additionalProperties") is False

    def test_properties_keys(self):
        props = DataCite.json_schema()["properties"]
        for key in ("identifier", "creators", "titles", "publisher", "publicationYear",
                    "resourceType", "contributors", "dates", "alternateIdentifiers",
                    "descriptions", "formats", "version", "subjects",
                    "relatedIdentifiers", "rightsList", "fundingReferences"):
            assert key in props

    def test_related_identifiers_not_required(self):
        assert "relatedIdentifiers" not in DataCite.json_schema()["required"]

    def test_dates_not_required(self):
        assert "dates" not in DataCite.json_schema()["required"]

    def test_optional_fields_not_required(self):
        required = DataCite.json_schema()["required"]
        for field in ("formats", "version", "subjects", "dates", "relatedIdentifiers",
                      "rightsList", "fundingReferences", "descriptions"):
            assert field not in required


# ---------------------------------------------------------------------------
# Serialisation (to_dict)
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_none_values_excluded(self, minimal_record):
        """Optional fields left as None must not appear in the serialised dict."""
        d = minimal_record.to_dict()
        creator = d["creators"][0]
        assert "affiliation" not in creator
        assert "name" not in creator

    def test_title_shorthand_produces_titles_array(self, minimal_record):
        d = minimal_record.to_dict()
        assert d["titles"] == [{"title": "A test dataset"}]

    def test_title_type_excluded_when_none(self, minimal_record):
        """Untyped (main) titles must not include a titleType key."""
        assert "titleType" not in minimal_record.to_dict()["titles"][0]

    def test_description_shorthand_produces_descriptions_array(self, minimal_record):
        d = minimal_record.to_dict()
        assert d["descriptions"] == [{
            "description": "A one-paragraph description.",
            "descriptionType": "Abstract",
        }]

    def test_date_type_enum_serialised_as_string(self, full_record):
        """DateType enum members must serialise to plain strings, not enum reprs."""
        d = full_record.to_dict()
        date_types = {entry["dateType"] for entry in d["dates"]}
        assert date_types == {"Created", "Submitted"}
        for dt in date_types:
            assert isinstance(dt, str)

    def test_empty_dates_list_serialised(self, minimal_record):
        d = minimal_record.to_dict()
        assert d["dates"] == []

    def test_no_identifier_when_absent(self, minimal_record):
        d = minimal_record.to_dict()
        assert "identifier" not in d  # excluded by exclude_none
        assert d["relatedIdentifiers"] == []

    def test_related_identifier_enums_serialised_as_strings(self, full_record):
        entry = full_record.to_dict()["relatedIdentifiers"][0]
        assert entry["relatedIdentifierType"] == "DOI"
        assert entry["relationType"] == "IsIdenticalTo"
        assert isinstance(entry["relatedIdentifierType"], str)
        assert isinstance(entry["relationType"], str)

    def test_roundtrip_validates_against_schema(self, minimal_record):
        """to_dict() output must satisfy the generated JSON Schema."""
        jsonschema.validate(
            instance=minimal_record.to_dict(),
            schema=DataCite.json_schema(),
        )

    def test_full_record_validates_against_schema(self, full_record):
        jsonschema.validate(
            instance=full_record.to_dict(),
            schema=DataCite.json_schema(),
        )


# ---------------------------------------------------------------------------
# title= shorthand
# ---------------------------------------------------------------------------

class TestTitleShorthand:
    def test_creates_titles_list(self):
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            title="My dataset",
            description="d",
            publisher=Publisher(name="pub"),
        )
        assert len(record.titles) == 1
        assert record.titles[0].title == "My dataset"
        assert record.titles[0].titleType is None

    def test_prepends_to_explicit_titles(self):
        extra = Title(title="Translated title", titleType=TitleType.TranslatedTitle)
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            title="Main title",
            description="d",
            publisher=Publisher(name="pub"),
            titles=[extra],
        )
        assert len(record.titles) == 2
        assert record.titles[0].title == "Main title"
        assert record.titles[1].titleType == TitleType.TranslatedTitle

    def test_none_is_ignored(self):
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            titles=[Title(title="Explicit")],
            descriptions=[Description(description="d", descriptionType=DescriptionType.Abstract)],
            publisher=Publisher(name="pub"),
            title=None,
        )
        assert len(record.titles) == 1


# ---------------------------------------------------------------------------
# description= shorthand
# ---------------------------------------------------------------------------

class TestDescriptionShorthand:
    def test_creates_descriptions_list(self):
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            title="t",
            description="A summary.",
            publisher=Publisher(name="pub"),
        )
        assert len(record.descriptions) == 1
        assert record.descriptions[0].description == "A summary."
        assert record.descriptions[0].descriptionType == DescriptionType.Abstract

    def test_prepends_to_explicit_descriptions(self):
        extra = Description(description="Method detail", descriptionType=DescriptionType.Methods)
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            title="t",
            description="Abstract text",
            publisher=Publisher(name="pub"),
            descriptions=[extra],
        )
        assert len(record.descriptions) == 2
        assert record.descriptions[0].descriptionType == DescriptionType.Abstract
        assert record.descriptions[1].descriptionType == DescriptionType.Methods

    def test_none_is_ignored(self):
        record = DataCite.new(
            creators=[Creator(familyName="S", givenName="J.")],
            titles=[Title(title="t")],
            descriptions=[Description(description="Explicit", descriptionType=DescriptionType.Abstract)],
            publisher=Publisher(name="pub"),
            description=None,
        )
        assert len(record.descriptions) == 1


# ---------------------------------------------------------------------------
# doi= shorthand
# ---------------------------------------------------------------------------

class TestDoiShorthand:
    def test_doi_kwarg_creates_identifier(self):
        record = DataCite.new(
            creators=[Creator(familyName="Smith", givenName="J.")],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
            doi="10.1234/example",
        )
        assert record.identifier is not None
        assert record.identifier.identifier == "10.1234/example"
        assert record.identifier.identifierType == "DOI"

    def test_doi_kwarg_does_not_affect_related_identifiers(self):
        record = DataCite.new(
            creators=[Creator(familyName="Smith", givenName="J.")],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
            doi="10.1234/example",
        )
        assert record.relatedIdentifiers == []

    def test_identifier_set_directly(self):
        ident = Identifier(identifier="10.9999/other", identifierType="DOI")
        record = DataCite.new(
            creators=[Creator(familyName="Smith", givenName="J.")],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
            identifier=ident,
        )
        assert record.identifier is not None and record.identifier.identifier == "10.9999/other"

    def test_doi_none_is_ignored(self):
        record = DataCite.new(
            creators=[Creator(familyName="Smith", givenName="J.")],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
            doi=None,
        )
        assert record.identifier is None

    def test_doi_shorthand_validates_against_schema(self):
        record = DataCite.new(
            creators=[Creator(familyName="Smith", givenName="J.")],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
            doi="10.1234/example",
        )
        jsonschema.validate(instance=record.to_dict(), schema=DataCite.json_schema())


# ---------------------------------------------------------------------------
# Pydantic type enforcement
# ---------------------------------------------------------------------------

class TestTypeEnforcement:
    def test_wrong_type_for_title_raises(self):
        with pytest.raises(ValidationError):
            DataCite.new(
                creators=[],
                titles=[12345],
                descriptions=[Description(description="d", descriptionType=DescriptionType.Abstract)],
                publisher=Publisher(name="pub"),
            )

    def test_extra_field_raises(self):
        with pytest.raises(ValidationError):
            DataCite.new(
                creators=[],
                title="t",
                description="d",
                publisher=Publisher(name="pub"),
                unexpected_field="value",
            )

    def test_wrong_type_for_date_string_raises(self):
        with pytest.raises(ValidationError):
            Date(date=20240101, dateType=DateType.Created)  # pyright: ignore[reportArgumentType]

    def test_invalid_date_type_string_raises(self):
        with pytest.raises(ValidationError):
            Date(date="2024-01-01T00:00:00Z", dateType="NotAValidType")  # pyright: ignore[reportArgumentType]

    def test_wrong_type_for_publisher_name_raises(self):
        with pytest.raises(ValidationError):
            Publisher(name=["not", "a", "string"])  # pyright: ignore[reportArgumentType]

    def test_invalid_description_type_raises(self):
        with pytest.raises(ValidationError):
            Description(description="text", descriptionType="NotAValidType")  # pyright: ignore[reportArgumentType]


# ---------------------------------------------------------------------------
# NameIdentifier
# ---------------------------------------------------------------------------

class TestNameIdentifier:
    def test_basic_orcid(self):
        ni = NameIdentifier(
            nameIdentifier="0000-0002-9072-1017",
            nameIdentifierScheme="ORCID",
            schemeUri="https://orcid.org",
        )
        assert ni.nameIdentifier == "0000-0002-9072-1017"
        assert ni.nameIdentifierScheme == "ORCID"
        assert ni.schemeUri == "https://orcid.org"

    def test_scheme_uri_optional(self):
        ni = NameIdentifier(
            nameIdentifier="0000000121032683",
            nameIdentifierScheme="ISNI",
        )
        assert ni.schemeUri is None

    def test_creator_with_name_identifiers(self):
        creator = Creator(
            familyName="Juno",
            givenName="Jennifer A.",
            nameIdentifiers=[
                NameIdentifier(
                    nameIdentifier="0000-0002-9072-1017",
                    nameIdentifierScheme="ORCID",
                    schemeUri="https://orcid.org",
                )
            ],
        )
        assert len(creator.nameIdentifiers) == 1
        assert creator.nameIdentifiers[0].nameIdentifierScheme == "ORCID"

    def test_creator_name_identifiers_default_empty(self):
        creator = Creator(familyName="Smith", givenName="J.")
        assert creator.nameIdentifiers == []

    def test_scheme_uri_excluded_when_none(self):
        creator = Creator(
            familyName="Smith",
            givenName="J.",
            nameIdentifiers=[
                NameIdentifier(nameIdentifier="0000000121032683", nameIdentifierScheme="ISNI")
            ],
        )
        record = DataCite.new(
            creators=[creator],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
        )
        ni_dict = record.to_dict()["creators"][0]["nameIdentifiers"][0]
        assert "schemeUri" not in ni_dict

    def test_name_identifier_missing_scheme_raises(self):
        with pytest.raises(ValidationError):
            NameIdentifier(nameIdentifier="0000-0002-9072-1017")  # pyright: ignore[reportCallIssue]

    def test_roundtrip_validates_against_schema(self):
        record = DataCite.new(
            creators=[Creator(
                familyName="Juno",
                givenName="Jennifer A.",
                nameIdentifiers=[NameIdentifier(
                    nameIdentifier="0000-0002-9072-1017",
                    nameIdentifierScheme="ORCID",
                    schemeUri="https://orcid.org",
                )],
            )],
            title="t",
            description="d",
            publisher=Publisher(name="pub"),
        )
        jsonschema.validate(instance=record.to_dict(), schema=DataCite.json_schema())
