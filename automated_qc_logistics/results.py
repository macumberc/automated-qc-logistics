"""Structured result helpers exposed as plain dicts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class GenieSpaceResult:
    """Outcome of Genie provisioning."""

    status: str
    requested: bool
    warehouse_id: Optional[str] = None
    title: Optional[str] = None
    parent_path: Optional[str] = None
    space_id: Optional[str] = None
    url: Optional[str] = None
    reason: Optional[str] = None
    replaced_space_ids: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DeploymentResult:
    """Top-level deployment return object."""

    catalog: str
    schema: str
    fqn: str
    seed: int
    schema_created: bool
    catalog_attempted: bool
    tables: dict[str, int]
    table_fqdns: dict[str, str]
    warehouse_id: Optional[str]
    genie: GenieSpaceResult

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["genie_url"] = self.genie.url
        return payload


@dataclass
class CleanupResult:
    """Cleanup outcome for schema and Genie resources."""

    catalog: Optional[str]
    schema: Optional[str]
    fqn: Optional[str]
    dropped_schema: bool
    deleted_space_ids: list[str]
    skipped_genie_cleanup: bool
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["deleted_space_count"] = len(self.deleted_space_ids)
        return payload
