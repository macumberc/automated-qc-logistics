"""Validation and default resolution helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .config import DEFAULT_SCHEMA_BASENAME

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Namespace:
    """Resolved deployment namespace."""

    username: str
    user_slug: str
    catalog: str
    schema: str

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}"


def sql_string(value: str) -> str:
    """Escape a Python string for single-quoted SQL literals."""
    return value.replace("'", "''")


def normalize_user_slug(username: str) -> str:
    """Convert the current user into a stable identifier-safe slug."""
    base = username.split("@", 1)[0]
    slug = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_").lower()
    if not slug:
        raise ValueError(
            "Could not derive a valid user slug from current_user(); "
            "pass an explicit schema name."
        )
    return slug


def validate_identifier(value: str, field_name: str) -> str:
    """Accept only simple unquoted identifiers for demo resources."""
    if not value:
        raise ValueError(f"{field_name} must not be empty.")
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{field_name}={value!r} is not a valid identifier. "
            "Use letters, numbers, and underscores only, and start with a letter "
            "or underscore."
        )
    return value


def default_schema_name(username: str) -> str:
    """Generate a user-scoped schema name."""
    return validate_identifier(
        f"{DEFAULT_SCHEMA_BASENAME}_{normalize_user_slug(username)}",
        "default schema",
    )


def current_user(spark) -> str:
    """Fetch the current Databricks user."""
    return spark.sql("SELECT current_user()").first()[0]


def current_catalog(spark) -> str:
    """Fetch the current Unity Catalog, falling back to main."""
    try:
        value = spark.sql("SELECT current_catalog()").first()[0]
    except Exception:
        value = "main"
    return validate_identifier(value, "catalog")


def catalog_exists(spark, catalog: str) -> bool:
    """Check whether a catalog already exists in the metastore."""
    rows = spark.sql("SHOW CATALOGS").collect()
    return any(row[0] == catalog for row in rows)


def resolve_namespace(
    spark,
    catalog: Optional[str],
    schema: Optional[str],
) -> Namespace:
    """Resolve user, catalog, and schema defaults."""
    username = current_user(spark)
    user_slug = normalize_user_slug(username)

    resolved_catalog = validate_identifier(
        catalog if catalog else current_catalog(spark),
        "catalog",
    )
    resolved_schema = validate_identifier(
        schema if schema else default_schema_name(username),
        "schema",
    )

    return Namespace(
        username=username,
        user_slug=user_slug,
        catalog=resolved_catalog,
        schema=resolved_schema,
    )
