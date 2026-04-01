"""Typed records and protocol contracts shared across the package."""

from .types import MaterializedRibResource, RibResource, RouteRecord, ValidationState, VrpObject, VrpSnapshot
from .interfaces import RibDiscovery, RibFetcher, RibParser, RouteRepository, RouteValidator

__all__ = [
    "MaterializedRibResource",
    "RibDiscovery",
    "RibFetcher",
    "RibParser",
    "RibResource",
    "RouteRecord",
    "RouteRepository",
    "RouteValidator",
    "ValidationState",
    "VrpObject",
    "VrpSnapshot",
]
