"""Deterministic SQL builders and domain metadata for QC inspection data."""

from __future__ import annotations

from .config import (
    CARRIERS,
    CAMERAS_PER_DOCK,
    DEFECT_TYPES,
    DOCKS_PER_FACILITY,
    DOCK_SCANS_TABLE,
    INSPECTION_TABLE,
    INSPECTION_TYPES,
    MODEL_PERF_TABLE,
    MODEL_VERSIONS,
    PRODUCTS,
    SEVERITIES,
    WAREHOUSES,
)

TABLE_COLUMN_COMMENTS = {
    INSPECTION_TABLE: {
        "inspection_id": "Unique inspection event identifier",
        "inspection_timestamp": "Timestamp when the CV inspection occurred",
        "facility_id": "Distribution center identifier (e.g. WH-EAST-01)",
        "dock_id": "Loading dock identifier within the facility (e.g. DOCK-01)",
        "camera_id": "Camera identifier on the dock (e.g. CAM-01)",
        "shipment_id": "Parent shipment being processed",
        "item_sku": "Product SKU code inspected",
        "product_name": "Human-readable product name",
        "product_category": (
            "Product category: Electronics, Food & Beverage, Home & Garden, "
            "Health & Wellness, Clothing & Apparel"
        ),
        "inspection_type": "Type of CV inspection: item_count, label_scan, damage_check",
        "result": "Inspection outcome: pass, fail, warning",
        "confidence_score": "Model confidence score between 0.00 and 1.00",
        "defect_type": (
            "Type of defect detected (NULL if passed): damaged_box, torn_label, "
            "sku_mismatch, count_discrepancy, crushed_corner, water_damage, "
            "missing_label, dented_container"
        ),
        "severity": "Defect severity when applicable: critical, major, minor",
        "processing_time_ms": "Inference latency in milliseconds",
    },
    DOCK_SCANS_TABLE: {
        "scan_id": "Unique scan session identifier",
        "shipment_id": "Shipment processed in this scan session",
        "scan_date": "Date the shipment was scanned",
        "facility_id": "Distribution center identifier",
        "dock_id": "Loading dock identifier",
        "direction": "Shipment direction: inbound or outbound",
        "carrier_name": (
            "Carrier: FedEx Freight, UPS Supply Chain, XPO Logistics, "
            "Old Dominion, SAIA, Estes Express"
        ),
        "expected_item_count": "Number of items expected per manifest",
        "verified_item_count": "Number of items verified by CV scan",
        "items_passed": "Items that passed all inspections",
        "items_failed": "Items that failed at least one inspection",
        "items_warning": "Items flagged with warnings",
        "sku_match_rate_pct": "Percentage of items with correct SKU labels",
        "damage_rate_pct": "Percentage of items with detected damage",
        "total_scan_duration_seconds": "Total time to process all items in shipment",
        "status": "Scan outcome: completed, flagged, rejected",
    },
    MODEL_PERF_TABLE: {
        "evaluation_date": "First day of the evaluation month",
        "model_name": "CV model name: item_counter, label_reader, damage_detector",
        "model_version": "Model version string (e.g. v2.0, v3.1, v4.2)",
        "inspection_type": "Inspection type this model handles",
        "facility_id": "Distribution center where model was evaluated",
        "precision_pct": "Precision as a percentage (true positives / predicted positives)",
        "recall_pct": "Recall as a percentage (true positives / actual positives)",
        "f1_score_pct": "F1 score as a percentage (harmonic mean of precision and recall)",
        "false_positive_rate_pct": "False positive rate as a percentage",
        "false_negative_rate_pct": "False negative rate as a percentage",
        "total_inferences": "Total number of inferences run during evaluation period",
        "avg_latency_ms": "Average inference latency in milliseconds",
    },
}


def table_fqdns(fqn: str) -> dict[str, str]:
    """Return fully qualified names for all managed tables."""
    return {
        INSPECTION_TABLE: f"{fqn}.{INSPECTION_TABLE}",
        DOCK_SCANS_TABLE: f"{fqn}.{DOCK_SCANS_TABLE}",
        MODEL_PERF_TABLE: f"{fqn}.{MODEL_PERF_TABLE}",
    }


def build_table_sqls(fqn: str, seed: int, scale: int = 1) -> dict[str, str]:
    """Build all deterministic CTAS statements."""
    return {
        INSPECTION_TABLE: build_inspection_events_sql(fqn, seed, scale),
        DOCK_SCANS_TABLE: build_dock_scans_sql(fqn, seed, scale),
        MODEL_PERF_TABLE: build_model_performance_sql(fqn, seed, scale),
    }


def _start_year(scale: int) -> int:
    return 2025 - (scale - 1)


def build_inspection_events_sql(fqn: str, seed: int, scale: int = 1) -> str:
    """Build the deterministic inspection events table."""

    start_year = _start_year(scale)
    products_values = _values_sql(
        [[p["sku"], p["name"], p["category"]] for p in PRODUCTS]
    )
    facility_values = _values_sql(
        [[w["warehouse_id"]] for w in WAREHOUSES]
    )
    dock_values = _values_sql(
        [[f"DOCK-{str(d).zfill(2)}"] for d in range(1, DOCKS_PER_FACILITY + 1)]
    )
    camera_values = _values_sql(
        [[f"CAM-{str(c).zfill(2)}"] for c in range(1, CAMERAS_PER_DOCK + 1)]
    )
    inspection_type_values = _values_sql([[t] for t in INSPECTION_TYPES])

    select_noise = _hash_fraction(seed, "insp_select", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", "it.itype")
    result_noise = _hash_fraction(seed, "insp_result", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", "it.itype")
    conf_noise = _hash_fraction(seed, "insp_conf", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", "it.itype")
    defect_noise = _hash_fraction(seed, "insp_defect", "d.inspection_date", "p.sku", "f.facility_id", "it.itype")
    severity_noise = _hash_fraction(seed, "insp_sev", "d.inspection_date", "p.sku", "f.facility_id", "it.itype")
    camera_noise = _hash_int(seed, "insp_cam", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", modulo=CAMERAS_PER_DOCK)
    shipment_idx = _hash_int(seed, "insp_shp", "d.inspection_date", "f.facility_id", "dk.dock_id", modulo=800)
    hour_noise = _hash_int(seed, "insp_hour", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", modulo=10, offset=6)
    minute_noise = _hash_int(seed, "insp_min", "d.inspection_date", "p.sku", "f.facility_id", "dk.dock_id", "it.itype", modulo=60)
    latency_noise = _hash_fraction(seed, "insp_lat", "d.inspection_date", "p.sku", "f.facility_id", "it.itype")

    return f"""
CREATE OR REPLACE TABLE {fqn}.{INSPECTION_TABLE} AS
WITH
products AS (
  SELECT * FROM VALUES
{products_values}
  AS t(sku, name, category)
),
facilities AS (
  SELECT * FROM VALUES
{facility_values}
  AS t(facility_id)
),
docks AS (
  SELECT * FROM VALUES
{dock_values}
  AS t(dock_id)
),
cameras AS (
  SELECT * FROM VALUES
{camera_values}
  AS t(camera_id)
),
inspection_types AS (
  SELECT * FROM VALUES
{inspection_type_values}
  AS t(itype)
),
date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'{start_year}-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS inspection_date
),
skeleton AS (
  SELECT
    d.inspection_date,
    p.sku, p.name, p.category,
    f.facility_id,
    dk.dock_id,
    it.itype,
    {select_noise} AS select_noise,
    {result_noise} AS result_noise,
    {conf_noise} AS conf_noise,
    {defect_noise} AS defect_noise,
    {severity_noise} AS severity_noise,
    {camera_noise} AS camera_idx,
    {shipment_idx} AS shipment_idx,
    {hour_noise} AS hour_val,
    {minute_noise} AS minute_val,
    {latency_noise} AS latency_noise,
    MONTH(d.inspection_date) AS mo,
    DAYOFWEEK(d.inspection_date) AS dow
  FROM date_range d
  CROSS JOIN products p
  CROSS JOIN facilities f
  CROSS JOIN docks dk
  CROSS JOIN inspection_types it
),
filtered AS (
  SELECT *,
    CASE
      WHEN category = 'Food & Beverage' AND itype = 'damage_check' THEN 0.0040
      WHEN category = 'Electronics' AND itype = 'label_scan' THEN 0.0035
      WHEN category = 'Electronics' AND mo IN (11, 12) THEN 0.0045
      WHEN category = 'Food & Beverage' THEN 0.0030
      WHEN category = 'Health & Wellness' THEN 0.0025
      WHEN category = 'Clothing & Apparel' AND mo IN (3, 4, 9, 10) THEN 0.0035
      WHEN category = 'Home & Garden' AND mo IN (4, 5, 6) THEN 0.0030
      ELSE 0.0020
    END AS selection_prob,
    CASE
      WHEN category = 'Food & Beverage' AND itype = 'damage_check' THEN 0.18
      WHEN category = 'Electronics' AND itype = 'label_scan' THEN 0.14
      WHEN category = 'Clothing & Apparel' AND itype = 'damage_check' THEN 0.10
      WHEN category = 'Health & Wellness' AND itype = 'damage_check' THEN 0.08
      WHEN itype = 'item_count' THEN 0.06
      WHEN itype = 'label_scan' THEN 0.09
      WHEN itype = 'damage_check' THEN 0.11
      ELSE 0.07
    END AS fail_prob,
    CASE
      WHEN mo IN (11, 12) THEN 1.3
      WHEN mo IN (6, 7) THEN 0.85
      ELSE 1.0
    END AS seasonal_factor
  FROM skeleton
  WHERE dow BETWEEN 2 AND 6
),
with_result AS (
  SELECT *,
    CASE
      WHEN result_noise < fail_prob * seasonal_factor THEN 'fail'
      WHEN result_noise < (fail_prob * seasonal_factor + 0.05) THEN 'warning'
      ELSE 'pass'
    END AS result,
    CONCAT('CAM-', LPAD(CAST(camera_idx + 1 AS STRING), 2, '0')) AS camera_id
  FROM filtered
  WHERE select_noise < selection_prob
),
with_defect AS (
  SELECT *,
    CASE
      WHEN result = 'pass' THEN NULL
      WHEN itype = 'item_count' THEN 'count_discrepancy'
      WHEN itype = 'label_scan' AND defect_noise < 0.40 THEN 'torn_label'
      WHEN itype = 'label_scan' AND defect_noise < 0.75 THEN 'sku_mismatch'
      WHEN itype = 'label_scan' THEN 'missing_label'
      WHEN itype = 'damage_check' AND defect_noise < 0.35 THEN 'damaged_box'
      WHEN itype = 'damage_check' AND defect_noise < 0.55 THEN 'crushed_corner'
      WHEN itype = 'damage_check' AND defect_noise < 0.75 THEN 'water_damage'
      ELSE 'dented_container'
    END AS defect_type,
    CASE
      WHEN result = 'pass' THEN NULL
      WHEN severity_noise < 0.15 THEN 'critical'
      WHEN severity_noise < 0.50 THEN 'major'
      ELSE 'minor'
    END AS severity
  FROM with_result
)
SELECT
  CONCAT('INS-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY inspection_date, facility_id, dock_id, sku, itype) + 10000 AS STRING), 7, '0')) AS inspection_id,
  CAST(CONCAT(
    CAST(inspection_date AS STRING), ' ',
    LPAD(CAST(hour_val AS STRING), 2, '0'), ':',
    LPAD(CAST(minute_val AS STRING), 2, '0'), ':00'
  ) AS TIMESTAMP) AS inspection_timestamp,
  facility_id,
  dock_id,
  camera_id,
  CONCAT('SHP-', LPAD(CAST(shipment_idx + 10000 AS STRING), 7, '0')) AS shipment_id,
  sku AS item_sku,
  name AS product_name,
  category AS product_category,
  itype AS inspection_type,
  result,
  ROUND(
    CASE
      WHEN result = 'pass' THEN 0.85 + 0.15 * conf_noise
      WHEN result = 'warning' THEN 0.55 + 0.25 * conf_noise
      ELSE 0.30 + 0.35 * conf_noise
    END, 2
  ) AS confidence_score,
  defect_type,
  severity,
  CAST(
    CASE
      WHEN itype = 'damage_check' THEN 120 + 180 * latency_noise
      WHEN itype = 'label_scan' THEN 80 + 120 * latency_noise
      ELSE 50 + 100 * latency_noise
    END AS INT
  ) AS processing_time_ms
FROM with_defect
""".strip()


def build_dock_scans_sql(fqn: str, seed: int, scale: int = 1) -> str:
    """Build the deterministic dock scans (shipment-level) table."""

    start_year = _start_year(scale)
    facility_values = _values_sql(
        [[w["warehouse_id"], w["warehouse_name"]] for w in WAREHOUSES]
    )
    dock_values = _values_sql(
        [[f"DOCK-{str(d).zfill(2)}"] for d in range(1, DOCKS_PER_FACILITY + 1)]
    )
    carrier_values = _values_sql([[c] for c in CARRIERS])

    select_noise = _hash_fraction(seed, "scan_select", "d.scan_date", "f.facility_id", "dk.dock_id")
    dir_noise = _hash_fraction(seed, "scan_dir", "d.scan_date", "f.facility_id", "dk.dock_id")
    carrier_idx = _hash_int(seed, "scan_carrier", "d.scan_date", "f.facility_id", "dk.dock_id", modulo=len(CARRIERS))
    expected_noise = _hash_fraction(seed, "scan_expected", "d.scan_date", "f.facility_id", "dk.dock_id")
    count_noise = _hash_fraction(seed, "scan_count", "d.scan_date", "f.facility_id", "dk.dock_id")
    fail_noise = _hash_fraction(seed, "scan_fail", "d.scan_date", "f.facility_id", "dk.dock_id")
    warn_noise = _hash_fraction(seed, "scan_warn", "d.scan_date", "f.facility_id", "dk.dock_id")
    sku_noise = _hash_fraction(seed, "scan_sku", "d.scan_date", "f.facility_id", "dk.dock_id")
    dmg_noise = _hash_fraction(seed, "scan_dmg", "d.scan_date", "f.facility_id", "dk.dock_id")
    dur_noise = _hash_fraction(seed, "scan_dur", "d.scan_date", "f.facility_id", "dk.dock_id")
    shipment_idx = _hash_int(seed, "scan_shp", "d.scan_date", "f.facility_id", "dk.dock_id", modulo=5000)

    return f"""
CREATE OR REPLACE TABLE {fqn}.{DOCK_SCANS_TABLE} AS
WITH
facilities AS (
  SELECT * FROM VALUES
{facility_values}
  AS t(facility_id, facility_name)
),
docks AS (
  SELECT * FROM VALUES
{dock_values}
  AS t(dock_id)
),
carriers AS (
  SELECT * FROM VALUES
{carrier_values}
  AS t(carrier_name)
),
carrier_indexed AS (
  SELECT carrier_name, ROW_NUMBER() OVER (ORDER BY carrier_name) - 1 AS carrier_idx
  FROM carriers
),
date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'{start_year}-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS scan_date
),
skeleton AS (
  SELECT
    d.scan_date,
    f.facility_id,
    dk.dock_id,
    {select_noise} AS select_noise,
    {dir_noise} AS dir_noise,
    {carrier_idx} AS carrier_idx,
    {expected_noise} AS expected_noise,
    {count_noise} AS count_noise,
    {fail_noise} AS fail_noise,
    {warn_noise} AS warn_noise,
    {sku_noise} AS sku_noise,
    {dmg_noise} AS dmg_noise,
    {dur_noise} AS dur_noise,
    {shipment_idx} AS shipment_idx,
    DAYOFWEEK(d.scan_date) AS dow
  FROM date_range d
  CROSS JOIN facilities f
  CROSS JOIN docks dk
),
filtered AS (
  SELECT *
  FROM skeleton
  WHERE dow BETWEEN 2 AND 6
    AND select_noise < 0.12
),
with_carrier AS (
  SELECT
    s.*,
    c.carrier_name
  FROM filtered s
  JOIN carrier_indexed c ON c.carrier_idx = s.carrier_idx
)
SELECT
  CONCAT('SCN-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY scan_date, facility_id, dock_id) + 10000 AS STRING), 7, '0')) AS scan_id,
  CONCAT('SHP-', LPAD(CAST(shipment_idx + 10000 AS STRING), 7, '0')) AS shipment_id,
  scan_date,
  facility_id,
  dock_id,
  CASE WHEN dir_noise < 0.55 THEN 'inbound' ELSE 'outbound' END AS direction,
  carrier_name,
  CAST(20 + FLOOR(expected_noise * 180) AS INT) AS expected_item_count,
  CAST(
    (20 + FLOOR(expected_noise * 180))
    * (0.96 + 0.04 * count_noise)
  AS INT) AS verified_item_count,
  CAST(
    (20 + FLOOR(expected_noise * 180)) * (0.82 + 0.15 * (1.0 - fail_noise))
  AS INT) AS items_passed,
  CAST(
    (20 + FLOOR(expected_noise * 180)) * (0.02 + 0.08 * fail_noise)
  AS INT) AS items_failed,
  CAST(
    (20 + FLOOR(expected_noise * 180)) * (0.01 + 0.04 * warn_noise)
  AS INT) AS items_warning,
  ROUND(92.0 + 7.0 * sku_noise, 1) AS sku_match_rate_pct,
  ROUND(
    CASE
      WHEN carrier_name IN ('SAIA', 'Estes Express') THEN 3.0 + 8.0 * dmg_noise
      WHEN carrier_name = 'XPO Logistics' THEN 2.5 + 6.0 * dmg_noise
      ELSE 1.0 + 4.0 * dmg_noise
    END, 1
  ) AS damage_rate_pct,
  CAST(120 + FLOOR(dur_noise * 480) AS INT) AS total_scan_duration_seconds,
  CASE
    WHEN fail_noise > 0.85 THEN 'rejected'
    WHEN fail_noise > 0.70 THEN 'flagged'
    ELSE 'completed'
  END AS status
FROM with_carrier
""".strip()


def build_model_performance_sql(fqn: str, seed: int, scale: int = 1) -> str:
    """Build the deterministic monthly model performance table."""

    start_year = _start_year(scale)
    facility_values = _values_sql(
        [[w["warehouse_id"]] for w in WAREHOUSES]
    )

    model_rows = []
    for inspection_type, model_name in [
        ("item_count", "item_counter"),
        ("label_scan", "label_reader"),
        ("damage_check", "damage_detector"),
    ]:
        for version in MODEL_VERSIONS[model_name]:
            model_rows.append([model_name, version, inspection_type])
    model_values = _values_sql(model_rows)

    prec_noise = _hash_fraction(seed, "perf_prec", "m.eval_date", "mv.model_name", "mv.model_version", "f.facility_id")
    rec_noise = _hash_fraction(seed, "perf_rec", "m.eval_date", "mv.model_name", "mv.model_version", "f.facility_id")
    fp_noise = _hash_fraction(seed, "perf_fp", "m.eval_date", "mv.model_name", "mv.model_version", "f.facility_id")
    fn_noise = _hash_fraction(seed, "perf_fn", "m.eval_date", "mv.model_name", "mv.model_version", "f.facility_id")
    infer_noise = _hash_fraction(seed, "perf_inf", "m.eval_date", "mv.model_name", "f.facility_id")
    lat_noise = _hash_fraction(seed, "perf_lat", "m.eval_date", "mv.model_name", "mv.model_version", "f.facility_id")
    pair_hash = _hash_int(seed, "perf_pair", "mv.model_name", "mv.model_version", "f.facility_id", modulo=100)

    return f"""
CREATE OR REPLACE TABLE {fqn}.{MODEL_PERF_TABLE} AS
WITH
model_versions AS (
  SELECT * FROM VALUES
{model_values}
  AS t(model_name, model_version, inspection_type)
),
facilities AS (
  SELECT * FROM VALUES
{facility_values}
  AS t(facility_id)
),
months AS (
  SELECT EXPLODE(SEQUENCE(DATE'{start_year}-01-01', DATE'2025-12-01', INTERVAL 1 MONTH)) AS eval_date
),
skeleton AS (
  SELECT
    m.eval_date,
    mv.model_name,
    mv.model_version,
    mv.inspection_type,
    f.facility_id,
    {prec_noise} AS prec_noise,
    {rec_noise} AS rec_noise,
    {fp_noise} AS fp_noise,
    {fn_noise} AS fn_noise,
    {infer_noise} AS infer_noise,
    {lat_noise} AS lat_noise,
    {pair_hash} AS pair_hash,
    CASE
      WHEN mv.model_version LIKE '%0' THEN 0
      WHEN mv.model_version LIKE '%1' THEN 1
      ELSE 2
    END AS version_rank,
    CASE
      WHEN f.facility_id IN ('WH-SOUTH-02', 'WH-WEST-02') THEN -3.0
      WHEN f.facility_id IN ('WH-EAST-01', 'WH-CENT-01') THEN 2.0
      ELSE 0.0
    END AS facility_bias
  FROM months m
  CROSS JOIN model_versions mv
  CROSS JOIN facilities f
),
filtered AS (
  SELECT *
  FROM skeleton
  WHERE pair_hash < 65
    AND (
      (model_version LIKE '%0') OR
      (model_version LIKE '%1' AND eval_date >= DATE'{start_year}-05-01') OR
      (model_version LIKE '%2' AND eval_date >= DATE'{start_year}-09-01')
    )
)
SELECT
  eval_date AS evaluation_date,
  model_name,
  model_version,
  inspection_type,
  facility_id,
  ROUND(GREATEST(70.0, LEAST(99.5,
    85.0 + version_rank * 3.5 + facility_bias + 8.0 * prec_noise
  )), 1) AS precision_pct,
  ROUND(GREATEST(65.0, LEAST(99.0,
    82.0 + version_rank * 3.0 + facility_bias + 10.0 * rec_noise
  )), 1) AS recall_pct,
  ROUND(GREATEST(67.0, LEAST(99.0,
    2.0 * (GREATEST(70.0, LEAST(99.5, 85.0 + version_rank * 3.5 + facility_bias + 8.0 * prec_noise)))
        * (GREATEST(65.0, LEAST(99.0, 82.0 + version_rank * 3.0 + facility_bias + 10.0 * rec_noise)))
    / NULLIF(
        (GREATEST(70.0, LEAST(99.5, 85.0 + version_rank * 3.5 + facility_bias + 8.0 * prec_noise)))
      + (GREATEST(65.0, LEAST(99.0, 82.0 + version_rank * 3.0 + facility_bias + 10.0 * rec_noise)))
    , 0)
  )), 1) AS f1_score_pct,
  ROUND(GREATEST(0.5, LEAST(15.0,
    8.0 - version_rank * 2.0 - facility_bias * 0.3 + 5.0 * fp_noise
  )), 1) AS false_positive_rate_pct,
  ROUND(GREATEST(0.5, LEAST(18.0,
    10.0 - version_rank * 2.5 - facility_bias * 0.3 + 6.0 * fn_noise
  )), 1) AS false_negative_rate_pct,
  CAST(800 + FLOOR(infer_noise * 4200) AS INT) AS total_inferences,
  CAST(
    CASE
      WHEN inspection_type = 'damage_check' THEN 140 + 80 * lat_noise
      WHEN inspection_type = 'label_scan' THEN 90 + 60 * lat_noise
      ELSE 55 + 50 * lat_noise
    END AS INT
  ) AS avg_latency_ms
FROM filtered
""".strip()


# ---------------------------------------------------------------------------
# SQL generation helpers (same deterministic pattern as reference repo)
# ---------------------------------------------------------------------------

def _values_sql(rows: list[list[object]]) -> str:
    """Render rows into a deterministic SQL VALUES block."""
    lines = []
    for idx, row in enumerate(rows):
        prefix = "   " if idx == 0 else "  , "
        rendered = ", ".join(_sql_value(value) for value in row)
        lines.append(f"{prefix}({rendered})")
    return "\n".join(lines)


def _sql_value(value: object) -> str:
    """Render a scalar as a SQL literal."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _hash_fraction(seed: int, salt: str, *parts: str, scale: int = 10000) -> str:
    """Create a deterministic pseudo-random decimal in [0, 1)."""
    sql_parts = ", ".join([f"'{seed}'", f"'{salt}'", *parts])
    return f"(CAST(pmod(hash({sql_parts}), {scale}) AS DOUBLE) / {scale}.0)"


def _hash_int(
    seed: int,
    salt: str,
    *parts: str,
    modulo: int,
    offset: int = 0,
) -> str:
    """Create a deterministic pseudo-random integer."""
    sql_parts = ", ".join([f"'{seed}'", f"'{salt}'", *parts])
    base = f"pmod(hash({sql_parts}), {modulo})"
    if offset == 0:
        return base
    if offset > 0:
        return f"({base} + {offset})"
    return f"({base} - {abs(offset)})"
