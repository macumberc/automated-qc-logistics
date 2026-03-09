"""Cleanup helpers for managed schemas and Genie spaces."""

from __future__ import annotations

from typing import Optional

from .genie import delete_genie_space, find_managed_spaces
from .results import CleanupResult
from .validators import validate_identifier


def cleanup(
    spark,
    deployment: Optional[dict] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    space_id: Optional[str] = None,
    drop_schema: bool = True,
    delete_genie: bool = True,
) -> dict:
    """Delete managed Genie spaces and optionally drop the demo schema."""

    resolved_catalog, resolved_schema, resolved_fqn = _resolve_namespace(
        deployment=deployment,
        catalog=catalog,
        schema=schema,
    )

    deleted_space_ids: list[str] = []
    skipped_genie_cleanup = not delete_genie
    notes: list[str] = []

    if delete_genie:
        target_space_ids = _resolve_space_ids(
            spark,
            deployment=deployment,
            fqn=resolved_fqn,
            explicit_space_id=space_id,
        )
        if not target_space_ids:
            skipped_genie_cleanup = True
            notes.append("No managed Genie spaces were found for cleanup.")
        else:
            for target_space_id in target_space_ids:
                try:
                    delete_genie_space(spark, target_space_id)
                    deleted_space_ids.append(target_space_id)
                except Exception as exc:
                    notes.append(f"Failed to delete Genie space {target_space_id}: {exc}")

    dropped_schema = False
    if drop_schema:
        if not resolved_fqn:
            raise ValueError(
                "cleanup(..., drop_schema=True) requires deployment metadata or "
                "explicit catalog and schema values."
            )
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {resolved_fqn} CASCADE")
            dropped_schema = True
        except Exception as exc:
            notes.append(f"Failed to drop schema {resolved_fqn}: {exc}")
    else:
        notes.append("Schema cleanup was skipped.")

    result = CleanupResult(
        catalog=resolved_catalog,
        schema=resolved_schema,
        fqn=resolved_fqn,
        dropped_schema=dropped_schema,
        deleted_space_ids=deleted_space_ids,
        skipped_genie_cleanup=skipped_genie_cleanup,
        notes=notes,
    )
    return result.as_dict()


def _resolve_namespace(
    deployment: Optional[dict],
    catalog: Optional[str],
    schema: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve catalog, schema, and fully-qualified schema name."""
    if deployment:
        catalog = catalog or deployment.get("catalog")
        schema = schema or deployment.get("schema")
        fqn = deployment.get("fqn")
        if catalog and schema:
            return catalog, schema, f"{catalog}.{schema}"
        if fqn and "." in fqn:
            dep_catalog, dep_schema = fqn.split(".", 1)
            return dep_catalog, dep_schema, fqn

    if catalog and schema:
        resolved_catalog = validate_identifier(catalog, "catalog")
        resolved_schema = validate_identifier(schema, "schema")
        return resolved_catalog, resolved_schema, f"{resolved_catalog}.{resolved_schema}"

    return None, None, None


def _resolve_space_ids(
    spark,
    deployment: Optional[dict],
    fqn: Optional[str],
    explicit_space_id: Optional[str],
) -> list[str]:
    """Find Genie spaces associated with the deployment."""
    if explicit_space_id:
        return [explicit_space_id]

    if deployment:
        genie = deployment.get("genie") or {}
        deployment_space_id = genie.get("space_id")
        if deployment_space_id:
            return [deployment_space_id]

    if fqn:
        return [
            space["space_id"]
            for space in find_managed_spaces(spark, fqn)
            if space.get("space_id")
        ]

    return []
