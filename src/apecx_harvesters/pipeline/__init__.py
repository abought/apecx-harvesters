"""Composable pipeline primitives for search, retrieval, and transformation."""

from .run import PipelineSpec, run, run_parallel
from .sinks import ReportResult, report, _to_gmetaentry, to_gmetalist
from .sources import csv_ids

__all__ = [
    "PipelineSpec",
    "run",
    "run_parallel",
    "ReportResult",
    "report",
    "_to_gmetaentry",
    "to_gmetalist",
    "csv_ids",
]