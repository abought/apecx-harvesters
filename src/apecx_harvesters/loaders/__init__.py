"""
Auto-import all loader subpackages so their harvester classes are registered
as BaseHarvester subclasses and discoverable via __subclasses__().

Adding a new subpackage under src/loaders/ is sufficient; no import list
needs to be updated.
"""

import importlib
import pkgutil
from pathlib import Path

for _pkg in pkgutil.iter_modules([str(Path(__file__).parent)]):
    if _pkg.ispkg:
        importlib.import_module(f"{__package__}.{_pkg.name}")

from .base.registry import SchemaRegistry as _SchemaRegistry  # noqa: E402

get_combined_schema = _SchemaRegistry.combined_json_schema
get_query_schema = _SchemaRegistry.query_json_schema