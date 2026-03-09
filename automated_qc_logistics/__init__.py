"""Automated QC Logistics — Genie Data Room for Databricks."""

from __future__ import annotations

from typing import Any, Optional

from .cleanup import cleanup as _cleanup
from .config import DEFAULT_SCHEMA_COMMENT, DEFAULT_SEED, VOLUME_NAME
from .data import TABLE_COLUMN_COMMENTS, build_table_sqls, table_fqdns
from .genie import create_or_replace_genie_space, resolve_warehouse_id
from .images import generate_and_upload_images
from .results import DeploymentResult, GenieSpaceResult
from .validators import catalog_exists, current_catalog, resolve_namespace, sql_string

try:
    from importlib.metadata import version as _version

    __version__ = _version("automated-qc-logistics")
except Exception:
    __version__ = "0.0.0-dev"


_PREFIX = "[automated-qc-logistics]"

_CATALOG_FALLBACK_ERRORS = ("PERMISSION_DENIED", "UNAUTHORIZED", "INVALID_STATE")


def _log(msg: str) -> None:
    print(f"{_PREFIX} {msg}")


def _display_html(html: str) -> None:
    try:
        import IPython  # type: ignore[import-untyped]

        ip = IPython.get_ipython()
        if ip and hasattr(ip, "user_ns") and "displayHTML" in ip.user_ns:
            ip.user_ns["displayHTML"](html)
    except Exception:
        pass


_SUMMARY_HTML = """\
<div style="font-family: 'Segoe UI', Roboto, sans-serif; max-width: 640px; padding: 20px;">
  <h2 style="color: #1b5e20; border-bottom: 2px solid #1b5e20; padding-bottom: 8px;">
    NorthStar Logistics &mdash; Automated QC Setup Complete
  </h2>
  <table style="border-collapse: collapse; width: 100%%; margin: 16px 0;">
    <tr style="background: #e8f5e9;">
      <td style="padding: 8px 12px; font-weight: bold;">Schema</td>
      <td style="padding: 8px 12px;">%(fqn)s</td>
    </tr>
    %(table_rows)s
    <tr style="background: #c8e6c9; font-weight: bold;">
      <td style="padding: 8px 12px;">Total</td>
      <td style="padding: 8px 12px;">%(total)s rows</td>
    </tr>
    %(image_row)s
  </table>
  %(genie_button)s
  <p style="margin-top: 16px; color: #666; font-size: 13px;">
    Cleanup &mdash; run this to remove everything:
  </p>
  <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px; font-size: 13px;">from automated_qc_logistics import teardown
teardown(spark, **result)</pre>
</div>
"""

_GENIE_BUTTON_HTML = """\
<div style="margin: 16px 0;">
  <a href="%(genie_url)s" target="_blank"
     style="display: inline-block; padding: 12px 24px; background: #1b5e20; color: white;
            text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 15px;">
    Open Genie Space
  </a>
</div>
"""


def deploy(
    spark: Any,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse_id: Optional[str] = "auto",
    seed: int = DEFAULT_SEED,
    scale: int = 1,
    generate_images: bool = True,
) -> dict[str, Any]:
    """Deploy the NorthStar Logistics automated QC Genie data room.

    Creates a Unity Catalog schema, generates three deterministic synthetic
    tables, optionally generates and uploads synthetic inspection images,
    and provisions a fully-configured Genie space.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (available as ``spark`` in Databricks notebooks).
    catalog : str, optional
        Target catalog. Defaults to the workspace's current catalog.
    schema : str, optional
        Target schema. Defaults to ``automated_qc_<user>``.
    warehouse_id : str, optional
        SQL warehouse ID for the Genie space. ``"auto"`` (default)
        auto-detects the best available warehouse. Pass ``None`` to skip
        Genie space creation entirely.
    seed : int
        Deterministic seed for data generation.
    scale : int
        Data size multiplier (default 1). ``scale=1`` generates 1 year of
        data (2025). ``scale=5`` generates 5 years (2021-2025), roughly
        5x the rows.
    generate_images : bool
        Whether to generate and upload synthetic inspection images to a
        UC Volume (default True).

    Returns
    -------
    dict
        Pass this dict as ``**result`` to :func:`teardown` to remove everything.
    """
    if scale < 1:
        raise ValueError("scale must be >= 1")
    ns = resolve_namespace(spark, catalog=catalog, schema=schema)

    _log(f"Catalog : {ns.catalog}")
    _log(f"Schema  : {ns.fqn}")

    catalog_attempted = False
    if catalog_exists(spark, ns.catalog):
        _log(f"Catalog '{ns.catalog}' already exists — skipping creation")
    else:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {ns.catalog}")
            catalog_attempted = True
        except Exception as exc:
            msg = str(exc)
            if any(err in msg for err in _CATALOG_FALLBACK_ERRORS):
                fallback = current_catalog(spark)
                _log(f"Cannot create catalog '{ns.catalog}' — falling back to '{fallback}'")
                ns = resolve_namespace(spark, catalog=fallback, schema=ns.schema)
                _log(f"Catalog : {ns.catalog}")
                _log(f"Schema  : {ns.fqn}")
            else:
                raise

    try:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
            f"COMMENT '{sql_string(DEFAULT_SCHEMA_COMMENT)}'"
        )
    except Exception as exc:
        msg = str(exc)
        if any(err in msg for err in _CATALOG_FALLBACK_ERRORS):
            fallback = current_catalog(spark)
            if ns.catalog != fallback:
                _log(f"Cannot create schema in '{ns.catalog}' — falling back to '{fallback}'")
                ns = resolve_namespace(spark, catalog=fallback, schema=ns.schema)
                _log(f"Catalog : {ns.catalog}")
                _log(f"Schema  : {ns.fqn}")
                spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
                    f"COMMENT '{sql_string(DEFAULT_SCHEMA_COMMENT)}'"
                )
            else:
                raise
        else:
            raise
    _log(f"Schema ready: {ns.fqn}")

    # --- Tables ---
    sqls = build_table_sqls(ns.fqn, seed, scale)
    tables: dict[str, int] = {}
    for name, sql in sqls.items():
        _log(f"Creating {name} ...")
        spark.sql(sql)
        cnt = spark.table(f"{ns.fqn}.{name}").count()
        tables[name] = cnt
        _log(f"  {name}: {cnt:,} rows")

    _log("Adding column comments ...")
    for table, cols in TABLE_COLUMN_COMMENTS.items():
        for col, comment in cols.items():
            spark.sql(
                f"ALTER TABLE {ns.fqn}.{table} "
                f"ALTER COLUMN {col} COMMENT '{sql_string(comment)}'"
            )
    _log("  Column comments applied")

    # --- Images ---
    volume_path: Optional[str] = None
    images_generated = 0
    if generate_images:
        _log("Generating synthetic inspection images ...")
        try:
            volume_path, images_generated = generate_and_upload_images(
                spark, ns.fqn, seed, log_fn=_log
            )
        except Exception as exc:
            _log(f"WARNING: Image generation failed: {exc}")
    else:
        _log("Image generation skipped")

    # --- Genie ---
    resolved_wh, skip_reason = resolve_warehouse_id(spark, warehouse_id)
    if skip_reason:
        _log(f"Genie: {skip_reason}")

    genie: GenieSpaceResult
    if resolved_wh:
        _log(f"Warehouse: {resolved_wh}")
        try:
            genie = create_or_replace_genie_space(
                spark, ns.fqn, resolved_wh, ns.username
            )
            _log(f"Genie space {genie.status}: {genie.url}")
        except Exception as exc:
            _log(f"WARNING: Genie space creation failed: {exc}")
            genie = GenieSpaceResult(
                status="failed", requested=True, reason=str(exc)
            )
    else:
        genie = GenieSpaceResult(
            status="skipped", requested=False, reason=skip_reason
        )

    result = DeploymentResult(
        catalog=ns.catalog,
        schema=ns.schema,
        fqn=ns.fqn,
        seed=seed,
        schema_created=True,
        catalog_attempted=catalog_attempted,
        tables=tables,
        table_fqdns=table_fqdns(ns.fqn),
        warehouse_id=resolved_wh,
        genie=genie,
        volume_path=volume_path,
        images_generated=images_generated,
    )

    print()
    _log("=" * 50)
    _log("SETUP COMPLETE")
    _log("=" * 50)
    total = sum(tables.values())
    for t, cnt in tables.items():
        _log(f"  {t:30s} {cnt:>6,} rows")
    _log(f"  {'TOTAL':30s} {total:>6,} rows")
    if images_generated:
        _log(f"  Images: {images_generated} uploaded to {volume_path}")
    if genie.url:
        _log(f"  Genie: {genie.url}")

    table_rows = "".join(
        f'<tr style="background: {"#f1f8e9" if i % 2 == 0 else "#ffffff"};">'
        f'<td style="padding: 8px 12px;">{t}</td>'
        f'<td style="padding: 8px 12px;">{cnt:,} rows</td>'
        f"</tr>"
        for i, (t, cnt) in enumerate(tables.items())
    )
    image_row = (
        f'<tr style="background: #e8f5e9;">'
        f'<td style="padding: 8px 12px;">Inspection Images</td>'
        f'<td style="padding: 8px 12px;">{images_generated} images</td>'
        f"</tr>"
        if images_generated
        else ""
    )
    genie_button = (
        (_GENIE_BUTTON_HTML % {"genie_url": genie.url}) if genie.url else ""
    )
    _display_html(
        _SUMMARY_HTML
        % {
            "fqn": ns.fqn,
            "table_rows": table_rows,
            "total": f"{total:,}",
            "image_row": image_row,
            "genie_button": genie_button,
        }
    )

    return result.as_dict()


def teardown(spark: Any, **kwargs: Any) -> dict[str, Any]:
    """Remove all resources created by :func:`deploy`.

    The easiest way to call this is to unpack the dict returned by ``deploy``::

        result = deploy(spark)
        teardown(spark, **result)
    """
    return _cleanup(spark, deployment=kwargs)


__all__ = ["deploy", "teardown"]
