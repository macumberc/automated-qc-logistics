"""Genie payload construction and workspace API helpers."""

from __future__ import annotations

import json
import os
from typing import Any, Optional
import urllib.error
import urllib.request

from .config import (
    AUTO_WAREHOUSE,
    DEFAULT_SPACE_TITLE,
    DOCK_SCANS_TABLE,
    HTTP_TIMEOUT_SECONDS,
    INSPECTION_TABLE,
    MODEL_PERF_TABLE,
    SPACE_DESCRIPTION_MARKER,
)
from .results import GenieSpaceResult


def build_space_title(fqn: str) -> str:
    """Render a user-facing Genie title."""
    return DEFAULT_SPACE_TITLE


def build_space_description(fqn: str) -> str:
    """Build a managed-space description with an embedded marker."""
    return (
        f"{SPACE_DESCRIPTION_MARKER}; fqn={fqn}\n\n"
        "Computer-vision quality control analytics for NorthStar Logistics. "
        "Ask questions about inspection results, defect rates, model accuracy, "
        "carrier quality, and scan throughput across 20 products and 8 "
        "distribution centers."
    )


def build_genie_payload(fqn: str, warehouse_id: str, username: str) -> dict[str, Any]:
    """Build the Genie REST payload for the QC data room."""

    serialized_space = {
        "version": 2,
        "config": {
            "sample_questions": [
                {
                    "id": "01f12000000000000000000000000001",
                    "question": ["Which products have the highest defect rates this month?"],
                },
                {
                    "id": "01f12000000000000000000000000002",
                    "question": ["What is the damage detection accuracy by facility?"],
                },
                {
                    "id": "01f12000000000000000000000000003",
                    "question": ["Show carriers with the most flagged shipments"],
                },
                {
                    "id": "01f12000000000000000000000000004",
                    "question": ["What is the average scan processing time by dock?"],
                },
                {
                    "id": "01f12000000000000000000000000005",
                    "question": ["Compare model precision across versions for damage detection"],
                },
            ]
        },
        "data_sources": {
            "tables": sorted([
                {
                    "identifier": f"{fqn}.{DOCK_SCANS_TABLE}",
                    "description": [
                        "Shipment-level scan summaries. Each row is one shipment processed "
                        "at a loading dock with item counts, pass/fail/warning breakdowns, "
                        "SKU match rates, damage rates, and carrier information."
                    ],
                    "column_configs": [
                        {"column_name": "carrier_name", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "direction", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "facility_id", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "scan_date", "enable_format_assistance": True},
                        {"column_name": "status", "enable_format_assistance": True, "enable_entity_matching": True},
                    ],
                },
                {
                    "identifier": f"{fqn}.{INSPECTION_TABLE}",
                    "description": [
                        "Individual CV inspection events at loading docks. Each row is one "
                        "item scanned by an AI camera for item counting, label verification, "
                        "or damage detection. Includes confidence scores, defect types, and "
                        "image paths."
                    ],
                    "column_configs": [
                        {"column_name": "defect_type", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "facility_id", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "inspection_timestamp", "enable_format_assistance": True},
                        {"column_name": "inspection_type", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "item_sku", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "product_category", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "result", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "severity", "enable_format_assistance": True, "enable_entity_matching": True},
                    ],
                },
                {
                    "identifier": f"{fqn}.{MODEL_PERF_TABLE}",
                    "description": [
                        "Monthly CV model performance metrics by facility. Tracks precision, "
                        "recall, F1 score, false positive/negative rates, and inference "
                        "latency across three model types and their versions."
                    ],
                    "column_configs": [
                        {"column_name": "evaluation_date", "enable_format_assistance": True},
                        {"column_name": "facility_id", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "inspection_type", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "model_name", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "model_version", "enable_format_assistance": True, "enable_entity_matching": True},
                    ],
                },
            ], key=lambda t: t["identifier"])
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": "01f12000000000000000000000000011",
                    "content": [
                        "You are a quality control analytics assistant for NorthStar Logistics, ",
                        "a nationwide 3PL provider with AI-powered cameras at loading docks across ",
                        "8 regional distribution centers.\n\n",
                        "For current inspection metrics, filter to the latest available dates.\n",
                        "Round percentages to 1 decimal place.\n",
                        "When computing defect rates, count results = 'fail' and 'warning' separately.\n",
                        "For model accuracy comparisons, use the f1_score_pct as the primary metric.\n",
                        "When asked about 'accuracy', use precision_pct and recall_pct from model_performance.\n",
                    ],
                }
            ],
            "example_question_sqls": [
                {
                    "id": "01f12000000000000000000000000021",
                    "question": ["What is the defect rate by product category?"],
                    "sql": [
                        f"SELECT product_category,\n",
                        f"  COUNT(*) AS total_inspections,\n",
                        f"  SUM(CASE WHEN result = 'fail' THEN 1 ELSE 0 END) AS failures,\n",
                        f"  ROUND(SUM(CASE WHEN result = 'fail' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS defect_rate_pct\n",
                        f"FROM {fqn}.{INSPECTION_TABLE}\n",
                        f"GROUP BY product_category\n",
                        f"ORDER BY defect_rate_pct DESC",
                    ],
                },
                {
                    "id": "01f12000000000000000000000000022",
                    "question": ["Which facilities have the highest false positive rate?"],
                    "sql": [
                        f"WITH latest AS (\n",
                        f"  SELECT * FROM {fqn}.{MODEL_PERF_TABLE}\n",
                        f"  WHERE evaluation_date = (SELECT MAX(evaluation_date) FROM {fqn}.{MODEL_PERF_TABLE})\n",
                        f")\n",
                        f"SELECT facility_id,\n",
                        f"  ROUND(AVG(false_positive_rate_pct), 1) AS avg_fp_rate\n",
                        f"FROM latest\n",
                        f"GROUP BY facility_id\n",
                        f"ORDER BY avg_fp_rate DESC",
                    ],
                },
                {
                    "id": "01f12000000000000000000000000023",
                    "question": ["Rank carriers by damage rate"],
                    "sql": [
                        f"SELECT carrier_name,\n",
                        f"  COUNT(*) AS total_scans,\n",
                        f"  ROUND(AVG(damage_rate_pct), 1) AS avg_damage_rate,\n",
                        f"  SUM(CASE WHEN status = 'flagged' THEN 1 ELSE 0 END) AS flagged_shipments,\n",
                        f"  SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected_shipments\n",
                        f"FROM {fqn}.{DOCK_SCANS_TABLE}\n",
                        f"GROUP BY carrier_name\n",
                        f"ORDER BY avg_damage_rate DESC",
                    ],
                },
                {
                    "id": "01f12000000000000000000000000024",
                    "question": ["Show daily inspection volume trend"],
                    "sql": [
                        f"SELECT DATE(inspection_timestamp) AS inspection_date,\n",
                        f"  COUNT(*) AS total_inspections,\n",
                        f"  SUM(CASE WHEN result = 'pass' THEN 1 ELSE 0 END) AS passed,\n",
                        f"  SUM(CASE WHEN result = 'fail' THEN 1 ELSE 0 END) AS failed\n",
                        f"FROM {fqn}.{INSPECTION_TABLE}\n",
                        f"GROUP BY DATE(inspection_timestamp)\n",
                        f"ORDER BY inspection_date",
                    ],
                },
                {
                    "id": "01f12000000000000000000000000025",
                    "question": ["How has the damage detector F1 score improved across versions?"],
                    "sql": [
                        f"SELECT model_version,\n",
                        f"  ROUND(AVG(f1_score_pct), 1) AS avg_f1,\n",
                        f"  ROUND(AVG(precision_pct), 1) AS avg_precision,\n",
                        f"  ROUND(AVG(recall_pct), 1) AS avg_recall,\n",
                        f"  ROUND(AVG(avg_latency_ms), 0) AS avg_latency\n",
                        f"FROM {fqn}.{MODEL_PERF_TABLE}\n",
                        f"WHERE model_name = 'damage_detector'\n",
                        f"GROUP BY model_version\n",
                        f"ORDER BY model_version",
                    ],
                },
            ],
            "join_specs": [],
            "sql_snippets": {
                "filters": [
                    {
                        "id": "01f12000000000000000000000000031",
                        "sql": ["inspection.result = 'fail'"],
                        "display_name": "failed inspections",
                        "synonyms": ["failures", "defects", "failed items"],
                        "instruction": ["Use to filter for items that failed QC inspection."],
                    },
                    {
                        "id": "01f12000000000000000000000000032",
                        "sql": ["inspection.severity = 'critical'"],
                        "display_name": "critical defects",
                        "synonyms": ["critical issues", "severe defects", "critical failures"],
                        "instruction": ["Use for the most severe quality issues requiring immediate attention."],
                    },
                    {
                        "id": "01f12000000000000000000000000033",
                        "sql": ["scans.direction = 'inbound'"],
                        "display_name": "inbound shipments",
                        "synonyms": ["incoming", "receiving", "inbound"],
                        "instruction": ["Use when analyzing shipments arriving at the facility."],
                    },
                ],
                "expressions": [
                    {
                        "id": "01f12000000000000000000000000041",
                        "alias": "inspection_month",
                        "sql": ["DATE_TRUNC('month', inspection.inspection_timestamp)"],
                        "display_name": "inspection month",
                        "synonyms": ["month", "monthly"],
                    },
                    {
                        "id": "01f12000000000000000000000000042",
                        "alias": "pass_rate",
                        "sql": [
                            "ROUND(SUM(CASE WHEN inspection.result = 'pass' THEN 1 ELSE 0 END) "
                            "* 100.0 / COUNT(*), 1)"
                        ],
                        "display_name": "pass rate",
                        "synonyms": ["quality rate", "success rate", "pass percentage"],
                    },
                    {
                        "id": "01f12000000000000000000000000043",
                        "alias": "defect_category",
                        "sql": [
                            "CASE WHEN inspection.defect_type IN ('damaged_box', 'crushed_corner', "
                            "'water_damage', 'dented_container') THEN 'Physical Damage' "
                            "WHEN inspection.defect_type IN ('torn_label', 'sku_mismatch', "
                            "'missing_label') THEN 'Label Issue' "
                            "WHEN inspection.defect_type = 'count_discrepancy' THEN 'Count Error' "
                            "ELSE 'None' END"
                        ],
                        "display_name": "defect category",
                        "synonyms": ["defect group", "issue type"],
                    },
                ],
                "measures": [
                    {
                        "id": "01f12000000000000000000000000051",
                        "alias": "total_inspections",
                        "sql": ["COUNT(*)"],
                        "display_name": "total inspections",
                        "synonyms": ["inspection count", "number of inspections"],
                    },
                    {
                        "id": "01f12000000000000000000000000052",
                        "alias": "avg_confidence",
                        "sql": ["ROUND(AVG(inspection.confidence_score), 3)"],
                        "display_name": "average confidence",
                        "synonyms": ["model confidence", "avg confidence score"],
                    },
                    {
                        "id": "01f12000000000000000000000000053",
                        "alias": "defect_rate_pct",
                        "sql": [
                            "ROUND(SUM(CASE WHEN inspection.result = 'fail' THEN 1 ELSE 0 END) "
                            "* 100.0 / NULLIF(COUNT(*), 0), 1)"
                        ],
                        "display_name": "defect rate",
                        "synonyms": ["failure rate", "defect percentage"],
                    },
                    {
                        "id": "01f12000000000000000000000000054",
                        "alias": "avg_scan_time",
                        "sql": ["ROUND(AVG(scans.total_scan_duration_seconds), 0)"],
                        "display_name": "average scan time",
                        "synonyms": ["scan duration", "processing time"],
                    },
                ],
            },
        },
        "benchmarks": {
            "questions": [
                {
                    "id": "01f12000000000000000000000000061",
                    "question": ["What is the defect rate by product category?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT product_category,\n",
                                f"  ROUND(SUM(CASE WHEN result = 'fail' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS defect_rate_pct\n",
                                f"FROM {fqn}.{INSPECTION_TABLE}\n",
                                f"GROUP BY product_category\n",
                                f"ORDER BY defect_rate_pct DESC",
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000062",
                    "question": ["Which model version has the best F1 score for damage detection?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT model_version, ROUND(AVG(f1_score_pct), 1) AS avg_f1\n",
                                f"FROM {fqn}.{MODEL_PERF_TABLE}\n",
                                f"WHERE model_name = 'damage_detector'\n",
                                f"GROUP BY model_version\n",
                                f"ORDER BY avg_f1 DESC",
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000063",
                    "question": ["Which carriers have the highest damage rate?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT carrier_name, ROUND(AVG(damage_rate_pct), 1) AS avg_damage_rate\n",
                                f"FROM {fqn}.{DOCK_SCANS_TABLE}\n",
                                f"GROUP BY carrier_name\n",
                                f"ORDER BY avg_damage_rate DESC",
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000064",
                    "question": ["What is the average scan processing time by facility?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT facility_id,\n",
                                f"  ROUND(AVG(total_scan_duration_seconds), 0) AS avg_scan_seconds\n",
                                f"FROM {fqn}.{DOCK_SCANS_TABLE}\n",
                                f"GROUP BY facility_id\n",
                                f"ORDER BY avg_scan_seconds DESC",
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000065",
                    "question": ["Which SKUs have the most critical defects?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT item_sku, product_name, COUNT(*) AS critical_defects\n",
                                f"FROM {fqn}.{INSPECTION_TABLE}\n",
                                f"WHERE severity = 'critical'\n",
                                f"GROUP BY item_sku, product_name\n",
                                f"ORDER BY critical_defects DESC",
                            ],
                        }
                    ],
                },
            ]
        },
    }

    return {
        "title": build_space_title(fqn),
        "description": build_space_description(fqn),
        "parent_path": f"/Workspace/Users/{username}",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized_space),
    }


# ---------------------------------------------------------------------------
# Warehouse resolution
# ---------------------------------------------------------------------------

def resolve_warehouse_id(spark, warehouse_id: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Resolve a warehouse id or return a skip reason."""
    if warehouse_id in (None, ""):
        return None, "Genie creation skipped because no warehouse_id was provided."

    if warehouse_id != AUTO_WAREHOUSE:
        return warehouse_id, None

    try:
        data = _api_request(spark, "GET", "/api/2.0/sql/warehouses")
    except RuntimeError as exc:
        return None, f"Warehouse auto-discovery failed: {exc}"

    warehouses = data.get("warehouses", []) if isinstance(data, dict) else []
    if not warehouses:
        return None, "No accessible SQL warehouses were found."

    ordered = sorted(warehouses, key=_warehouse_sort_key)
    candidate = ordered[0]
    return candidate.get("id"), None


# ---------------------------------------------------------------------------
# Genie CRUD
# ---------------------------------------------------------------------------

def create_or_replace_genie_space(
    spark,
    fqn: str,
    warehouse_id: str,
    username: str,
) -> GenieSpaceResult:
    """Delete any prior managed space for the namespace and create a fresh one."""
    existing = find_managed_spaces(spark, fqn)
    replaced_ids: list[str] = []
    for space in existing:
        space_id = space.get("space_id")
        if space_id:
            delete_genie_space(spark, space_id)
            replaced_ids.append(space_id)

    payload = build_genie_payload(fqn, warehouse_id, username)
    created = _api_request(
        spark,
        "POST",
        "/api/2.0/genie/spaces",
        payload=payload,
        expected_statuses=(200, 201),
    )
    space_id = created["space_id"]
    workspace_url = _workspace_url(spark)

    return GenieSpaceResult(
        status="replaced" if replaced_ids else "created",
        requested=True,
        warehouse_id=warehouse_id,
        title=payload["title"],
        parent_path=payload["parent_path"],
        space_id=space_id,
        url=f"https://{workspace_url}/genie/rooms/{space_id}",
        replaced_space_ids=replaced_ids,
    )


def find_managed_spaces(spark, fqn: str) -> list[dict[str, Any]]:
    """List spaces owned by this package for the target namespace."""
    data = _api_request(spark, "GET", "/api/2.0/genie/spaces")
    spaces = data.get("spaces", []) if isinstance(data, dict) else []
    marker = f"fqn={fqn}"
    title = build_space_title(fqn)

    results = []
    for space in spaces:
        description = space.get("description", "") or ""
        if marker in description or space.get("title") == title:
            results.append(space)
    return results


def delete_genie_space(spark, space_id: str) -> None:
    """Delete a Genie space."""
    _api_request(
        spark,
        "DELETE",
        f"/api/2.0/genie/spaces/{space_id}",
        expected_statuses=(200, 202, 204),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _warehouse_sort_key(warehouse: dict[str, Any]) -> tuple[Any, ...]:
    """Prefer running, serverless-ish, and smaller warehouses."""
    size_rank = {
        "2X-Small": 0, "X-Small": 1, "Small": 2, "Medium": 3,
        "Large": 4, "X-Large": 5, "2X-Large": 6,
    }
    name = (warehouse.get("name") or "").lower()
    return (
        warehouse.get("state") != "RUNNING",
        "serverless" not in name,
        "starter" not in name,
        "shared" not in name,
        size_rank.get(warehouse.get("cluster_size"), 99),
        name,
    )


def _api_request(
    spark,
    method: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    expected_statuses: tuple[int, ...] = (200,),
) -> Any:
    """Issue a Databricks workspace REST request using notebook auth."""
    workspace_url = _workspace_url(spark)
    token = _api_token(spark)

    request_body = None
    if payload is not None:
        request_body = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url=f"https://{workspace_url}{path}",
        data=request_body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status_code = response.getcode()
            response_text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {path} failed with status {exc.code}: {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc}") from exc

    if status_code not in expected_statuses:
        raise RuntimeError(
            f"{method} {path} failed with status {status_code}: {response_text}"
        )

    if not response_text:
        return {}
    return json.loads(response_text)


def _workspace_url(spark) -> str:
    """Resolve the current workspace URL without protocol."""
    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception as exc:
        raise RuntimeError(f"Could not resolve spark.databricks.workspaceUrl: {exc}") from exc


def _api_token(spark) -> str:
    """Resolve an API token from dbutils or the environment."""
    dbutils = _get_dbutils(spark)
    if dbutils is not None:
        try:
            return (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .apiToken()
                .get()
            )
        except Exception:
            pass

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token

    raise RuntimeError(
        "Could not obtain a Databricks API token from dbutils or DATABRICKS_TOKEN."
    )


def _get_dbutils(spark):
    """Resolve dbutils when running inside a Databricks environment."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            return None
