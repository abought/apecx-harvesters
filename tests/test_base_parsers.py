"""
Unit tests for shared parser utilities in loaders/base/parser.py.

These functions are used across multiple harvesters; testing them centrally
avoids repeating the same assertions in every harvester test suite.
"""

from __future__ import annotations

from apecx_harvesters.loaders.base.parser import (
    build_journal_related_item,
    compose_creator_name,
    deduplicate_subjects,
    orcid_name_identifier,
    split_page,
)
from apecx_harvesters.loaders.base import RelatedIdentifierType, RelatedItemType, RelationType


# ---------------------------------------------------------------------------
# orcid_name_identifier
# ---------------------------------------------------------------------------

class TestOrcidNameIdentifier:
    def test_scheme_is_orcid(self):
        ni = orcid_name_identifier("0000-0002-1234-5678")
        assert ni.nameIdentifierScheme == "ORCID"

    def test_scheme_uri(self):
        ni = orcid_name_identifier("0000-0002-1234-5678")
        assert ni.schemeUri == "https://orcid.org"

    def test_bare_id_passed_through(self):
        ni = orcid_name_identifier("0000-0002-1234-5678")
        assert ni.nameIdentifier == "0000-0002-1234-5678"

    def test_https_url_stripped(self):
        ni = orcid_name_identifier("https://orcid.org/0000-0002-1234-5678")
        assert ni.nameIdentifier == "0000-0002-1234-5678"

    def test_http_url_stripped(self):
        ni = orcid_name_identifier("http://orcid.org/0000-0002-1234-5678")
        assert ni.nameIdentifier == "0000-0002-1234-5678"


# ---------------------------------------------------------------------------
# compose_creator_name
# ---------------------------------------------------------------------------

class TestComposeCreatorName:
    def test_both_parts(self):
        assert compose_creator_name("Smith", "John") == "Smith, John"

    def test_family_only(self):
        assert compose_creator_name("Smith", None) == "Smith"

    def test_given_only(self):
        assert compose_creator_name(None, "John") == "John"

    def test_neither(self):
        assert compose_creator_name(None, None) is None

    def test_empty_strings_treated_as_none(self):
        assert compose_creator_name("", "") is None


# ---------------------------------------------------------------------------
# split_page
# ---------------------------------------------------------------------------

class TestSplitPage:
    def test_none_input(self):
        assert split_page(None) == (None, None)

    def test_empty_string(self):
        assert split_page("") == (None, None)

    def test_single_page(self):
        assert split_page("42") == ("42", None)

    def test_page_range(self):
        assert split_page("47-55") == ("47", "55")

    def test_range_with_empty_first(self):
        assert split_page("-55") == (None, "55")

    def test_range_with_empty_last(self):
        assert split_page("47-") == ("47", None)

    def test_splits_on_first_hyphen_only(self):
        # "e123-e130" — only the first hyphen is the separator
        first, last = split_page("e123-e130")
        assert first == "e123"
        assert last == "e130"


# ---------------------------------------------------------------------------
# build_journal_related_item
# ---------------------------------------------------------------------------

class TestBuildJournalRelatedItem:
    def test_returns_none_when_no_fields(self):
        result = build_journal_related_item(
            title="Some Journal", issn=None, volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is None

    def test_returns_item_when_issn_present(self):
        result = build_journal_related_item(
            title="Nature", issn="0028-0836", volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None

    def test_returns_item_when_volume_only(self):
        result = build_journal_related_item(
            title=None, issn=None, volume="12", issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None

    def test_relation_type(self):
        result = build_journal_related_item(
            title=None, issn="1234-5678", volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None
        assert result.relationType == RelationType.IsPublishedIn
        assert result.relatedItemType == RelatedItemType.Journal

    def test_issn_identifier(self):
        result = build_journal_related_item(
            title=None, issn="1234-5678", volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None
        assert result.relatedItemIdentifier is not None
        assert result.relatedItemIdentifier.relatedItemIdentifier == "1234-5678"
        assert result.relatedItemIdentifier.relatedItemIdentifierType == RelatedIdentifierType.ISSN

    def test_no_issn_identifier_when_absent(self):
        result = build_journal_related_item(
            title=None, issn=None, volume="5", issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None
        assert result.relatedItemIdentifier is None

    def test_title_in_titles_list(self):
        result = build_journal_related_item(
            title="Nature", issn="0028-0836", volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None
        assert result.titles[0].title == "Nature"

    def test_no_titles_when_absent(self):
        result = build_journal_related_item(
            title=None, issn="0028-0836", volume=None, issue=None,
            first_page=None, last_page=None,
        )
        assert result is not None
        assert result.titles == []

    def test_volume_issue_pages(self):
        result = build_journal_related_item(
            title=None, issn="0028-0836", volume="12", issue="3",
            first_page="100", last_page="110",
        )
        assert result is not None
        assert result.volume == "12"
        assert result.issue == "3"
        assert result.firstPage == "100"
        assert result.lastPage == "110"


# ---------------------------------------------------------------------------
# deduplicate_subjects
# ---------------------------------------------------------------------------

class TestDeduplicateSubjects:
    def test_basic(self):
        results = deduplicate_subjects(["biology", "chemistry"])
        assert [s.subject for s in results] == ["biology", "chemistry"]

    def test_deduplication(self):
        results = deduplicate_subjects(["biology", "chemistry", "biology"])
        assert [s.subject for s in results] == ["biology", "chemistry"]

    def test_empty_strings_skipped(self):
        results = deduplicate_subjects(["biology", "", "chemistry"])
        assert len(results) == 2

    def test_whitespace_only_skipped(self):
        results = deduplicate_subjects(["biology", "   ", "chemistry"])
        assert len(results) == 2

    def test_whitespace_stripped_before_dedup(self):
        results = deduplicate_subjects(["biology", " biology "])
        assert len(results) == 1
        assert results[0].subject == "biology"

    def test_empty_iterable(self):
        assert deduplicate_subjects([]) == []

    def test_preserves_insertion_order(self):
        terms = ["zebra", "apple", "mango"]
        results = deduplicate_subjects(terms)
        assert [s.subject for s in results] == terms

    def test_case_sensitive(self):
        results = deduplicate_subjects(["Biology", "biology"])
        assert len(results) == 2