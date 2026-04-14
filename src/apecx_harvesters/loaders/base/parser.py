"""Shared parser utilities used across multiple harvesters."""

from __future__ import annotations

from collections.abc import Iterable

from .model import (
    NameIdentifier,
    RelatedIdentifierType,
    RelatedItem,
    RelatedItemIdentifier,
    RelatedItemType,
    RelationType,
    Subject,
    Title,
)


def parse_author_name(name: str) -> tuple[str, str | None]:
    """Parse a human name string into ``(family, given)`` components.

    Accepts any of these formats::

        "Jane Smith"   → ("Smith", "Jane")
        "Smith, Jane"  → ("Smith", "Jane")
        "J. Smith"     → ("Smith", "J")
        "Smith"        → ("Smith", None)
    """
    name = name.strip()
    if "," in name:
        family, _, rest = name.partition(",")
        given = rest.strip().rstrip(".")
        return family.strip(), given or None

    parts = name.split()
    if len(parts) == 1:
        return parts[0], None

    family = parts[-1]
    given = parts[0].rstrip(".")
    return family, given or None


def orcid_name_identifier(raw: str) -> NameIdentifier:
    """Build a NameIdentifier for an ORCID value.

    Accepts either a bare ID (``"0000-0002-1234-5678"``) or a full URL
    (``"https://orcid.org/0000-0002-1234-5678"``); the URL prefix is stripped
    when present.
    """
    orcid_id = raw.split("orcid.org/")[-1]
    return NameIdentifier(
        nameIdentifier=orcid_id,
        nameIdentifierScheme="ORCID",
        schemeUri="https://orcid.org",
    )


def compose_creator_name(family: str | None, given: str | None) -> str | None:
    """Format a display name from family and given name components.

    Returns ``"Family, Given"`` when both are present, the non-None component
    when only one is set, and ``None`` when neither is set.
    """
    if family and given:
        return f"{family}, {given}"
    return family or given or None


def split_page(page: str | None) -> tuple[str | None, str | None]:
    """Split a page-range string into ``(first_page, last_page)``.

    A range like ``"47-55"`` returns ``("47", "55")``.  A single page like
    ``"1065"`` returns ``("1065", None)``.  ``None`` or empty string returns
    ``(None, None)``.
    """
    if not page:
        return None, None
    if "-" in page:
        parts = page.split("-", 1)
        return parts[0] or None, parts[1] or None
    return page, None


def build_journal_related_item(
    *,
    title: str | None,
    issn: str | None,
    volume: str | None,
    issue: str | None,
    first_page: str | None,
    last_page: str | None,
) -> RelatedItem | None:
    """Build a ``Journal / IsPublishedIn`` RelatedItem from container fields.

    Returns ``None`` when no substantive container information is present
    (i.e. all of issn, volume, issue, and first_page are absent).
    """
    if not any([issn, volume, issue, first_page]):
        return None
    return RelatedItem(
        relatedItemType=RelatedItemType.Journal,
        relationType=RelationType.IsPublishedIn,
        relatedItemIdentifier=RelatedItemIdentifier(
            relatedItemIdentifier=issn,
            relatedItemIdentifierType=RelatedIdentifierType.ISSN,
        ) if issn else None,
        titles=[Title(title=title)] if title else [],
        volume=volume,
        issue=issue,
        firstPage=first_page,
        lastPage=last_page,
    )


def deduplicate_subjects(terms: Iterable[str]) -> list[Subject]:
    """Build a deduplicated ``Subject`` list from an iterable of strings.

    Blank and whitespace-only strings are silently skipped.  Case-sensitive
    deduplication preserves insertion order.
    """
    seen: set[str] = set()
    subjects = []
    for term in terms:
        term = term.strip()
        if term and term not in seen:
            subjects.append(Subject(subject=term))
            seen.add(term)
    return subjects