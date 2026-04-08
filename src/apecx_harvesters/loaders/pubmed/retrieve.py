"""PubMed harvester."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import ClassVar

from ..base import BaseHarvester
from .constants import rate_limit
from .model import PubMedContainer
from .parser import _parse_article

_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# NCBI recommends batches of up to 500 IDs per efetch request.
_BATCH_SIZE = 500


class PubMedHarvester(BaseHarvester):
    _BATCH_SIZE: ClassVar[int] = _BATCH_SIZE
    _CACHE_DIR = "pubmed"
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        # POST avoids 414 URI Too Long when batch has many IDs (NCBI recommends POST for large lists)
        body = f"db=pubmed&id={','.join(ids)}&retmode=xml"
        return _EFETCH_URL, body, {"Content-Type": "application/x-www-form-urlencoded"}

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        """Split an efetch multi-record XML response into per-PMID article XML strings."""
        root = ET.fromstring(content)
        result: dict[str, str] = {}
        for elem in root.findall(".//PubmedArticle"):
            pmid = elem.findtext(".//PMID")
            if pmid:
                result[pmid] = ET.tostring(elem, encoding="unicode")
        for elem in root.findall(".//PubmedBookArticle"):
            pmid = elem.findtext(".//PMID")
            if pmid:
                result[pmid] = ET.tostring(elem, encoding="unicode")
        return result

    async def _parse_item(self, content: str) -> PubMedContainer:
        root = ET.fromstring(content)
        if root.tag == "PubmedBookArticle":
            pmid = root.findtext(".//PMID") or "unknown"
            raise ValueError(f"PMID {pmid}: PubmedBookArticle entries are not yet supported")
        return _parse_article(root)