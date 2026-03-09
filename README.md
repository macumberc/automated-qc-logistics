# Computer Vision Automated QC — Genie Data Room

A ready-to-deploy Databricks Genie space for computer-vision automated quality control at logistics loading docks. Two lines in a serverless notebook gives you a fully configured Genie room backed by three synthetic Unity Catalog tables and ~100 synthetic inspection images.

## Quick Start

```python
# Cell 1
%pip install git+https://github.com/macumberc/automated-qc-logistics.git

# Cell 2
from automated_qc_logistics import deploy
result = deploy(spark)
```

That's it. `deploy()` creates the schema, three deterministic tables, generates synthetic inspection images in a UC Volume, auto-detects the best SQL warehouse, creates a Genie space, and renders clickable buttons for the Genie room and cleanup.

To scale up the data size:

```python
result = deploy(spark, scale=5)  # 5 years of data
```

To skip image generation (faster deploy):

```python
result = deploy(spark, generate_images=False)
```

To use a specific warehouse:

```python
result = deploy(spark, warehouse_id="your_warehouse_id")
```

## Cleanup

```python
from automated_qc_logistics import teardown
teardown(spark, **result)
```

This drops the schema (CASCADE), deletes the UC Volume with all images, and removes the Genie space.

## Scenario

**NorthStar Logistics** is a nationwide third-party logistics (3PL) provider operating 8 regional distribution centers. The company has deployed AI-powered cameras at every loading dock to automate quality control. The operations team needs visibility into inspection results, defect patterns, model accuracy, and carrier quality to reduce damage claims ($1.8M/year) and improve dock-to-stock time.

The CV system performs three types of inspections:
- **Item Count** — Scan and count items in real-time, flag discrepancies against the manifest.
- **Label Scan** — Verify labels and validate SKUs against expected shipment data.
- **Damage Check** — Detect damaged packaging (crushed corners, water damage, dented containers).

## API Reference

### `deploy(spark, catalog=None, schema=None, warehouse_id="auto", seed=20250309, scale=1, generate_images=True)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| spark | SparkSession | *required* | Active Spark session (`spark` in notebooks) |
| catalog | str | current catalog | Target catalog name |
| schema | str | `automated_qc_<user>` | Target schema name (user-scoped by default) |
| warehouse_id | str | `"auto"` | SQL warehouse ID for Genie space; `"auto"` selects the best available, `None` skips Genie creation |
| seed | int | 20250309 | Deterministic seed for data generation |
| scale | int | 1 | Data size multiplier. 1 = 1 year (2025), 5 = 5 years (2021–2025). Rows scale linearly. |
| generate_images | bool | True | Whether to generate and upload synthetic inspection images |

**Returns** a dict with keys: `catalog`, `schema`, `fqn`, `seed`, `tables`, `table_fqdns`, `warehouse_id`, `genie`, `genie_url`, `volume_path`, `images_generated`.

### `teardown(spark, **result)`

Accepts the dict returned by `deploy()` via `**result` unpacking.

## What Gets Created

### Tables

| Table | Rows (scale=1) | Description |
|---|---|---|
| `inspection_events` | ~2,500 | Individual CV inspection results — item counts, label scans, and damage checks with confidence scores, defect types, and image paths |
| `dock_scans` | ~1,200 | Shipment-level scan summaries — expected vs verified counts, pass/fail/warning breakdowns, carrier info, SKU match rates |
| `model_performance` | ~600 | Monthly CV model accuracy — precision, recall, F1 score, false positive/negative rates, and latency across 3 model types |

### Inspection Images

~100 synthetic PNG images stored in a UC Volume at `/Volumes/{catalog}/{schema}/inspection_images/`. Each image simulates a dock camera capture of a shipping box with:
- Product label with category-coded color stripe and barcode
- Camera HUD overlay (camera ID, inspection type, status)
- **Pass images**: Clean box with green PASS banner
- **Fail images**: Defect-specific overlays (scuff marks, torn labels, red X for SKU mismatches, count discrepancy warnings, water stains, etc.)

The `inspection_events.image_path` column references these images for each inspection row.

### Products (20 SKUs across 5 categories)

| Category | SKUs | Example |
|---|---|---|
| Electronics | 5 | Wireless Bluetooth Speaker, USB-C Hub, Noise-Cancelling Earbuds |
| Food & Beverage | 4 | Organic Protein Bars, Cold Brew Coffee, Sparkling Water |
| Home & Garden | 4 | Smart LED Bulbs, Bamboo Cutting Board, Indoor Herb Garden Kit |
| Health & Wellness | 4 | Vitamin D3, Melatonin Gummies, Probiotic Capsules |
| Clothing & Apparel | 3 | Running Socks, Compression Tights, Hiking Shorts |

### Warehouses (8 distribution centers)

| ID | Name | Region |
|---|---|---|
| WH-EAST-01 | Newark DC | Northeast |
| WH-EAST-02 | Atlanta DC | Southeast |
| WH-CENT-01 | Chicago DC | Midwest |
| WH-CENT-02 | Minneapolis DC | Great Lakes |
| WH-SOUTH-01 | Dallas DC | South Central |
| WH-SOUTH-02 | Miami DC | Southeast |
| WH-WEST-01 | Los Angeles DC | Pacific |
| WH-WEST-02 | Denver DC | Mountain |

### CV Models

| Model | Inspection Type | Versions | Description |
|---|---|---|---|
| item_counter | item_count | v2.0, v2.1, v2.2 | Counts items and verifies against manifest |
| label_reader | label_scan | v3.0, v3.1, v3.2 | OCR-based label verification and SKU matching |
| damage_detector | damage_check | v4.0, v4.1, v4.2 | Visual damage detection on packaging |

### Genie Space Configuration

The Genie space is deployed with:
- **General instructions** — role context and QC-specific query guidelines
- **5 sample questions** displayed on the landing page
- **5 example SQL queries** covering defect rates, model accuracy, carrier ranking, inspection trends, and F1 improvement
- **SQL snippets** — 3 filters, 3 expressions, 4 measures
- **5 benchmarks** with expected SQL for accuracy testing

## Example Questions to Ask Genie

- Which products have the highest defect rates this month?
- What is the damage detection accuracy by facility?
- Show carriers with the most flagged shipments.
- What is the average scan processing time by dock?
- Compare model precision across versions for damage detection.
- Which facilities have the highest false positive rate?
- What percentage of inbound shipments are rejected?
- Show the trend of critical defects over time.
- Which SKUs have the most label mismatches?
- What is the average confidence score by inspection type?

## Data Patterns

The synthetic data includes realistic patterns:
- **Food & Beverage** items have higher damage rates (fragile packaging)
- **Electronics** have higher label mismatch rates (complex labeling)
- **Seasonal spikes**: Nov/Dec holiday volume increases error rates by 30%
- **Carrier quality**: SAIA and Estes Express have consistently higher damage rates
- **Model improvement**: newer versions show 3-7% F1 improvement
- **Facility variance**: Miami and Denver DCs perform slightly worse (lighting/conditions)
- **Confidence correlation**: lower confidence scores correlate with detected defects

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Permission to create schemas in a catalog
- A SQL Pro or Serverless SQL warehouse (only needed for Genie space creation)
- DBR 13.3+ or serverless notebook

## Resources

- [Curate an effective Genie space](https://docs.databricks.com/en/genie/best-practices.html)
- [How to Build Production-Ready Genie Spaces](https://community.databricks.com/t5/technical-blog/how-to-build-production-ready-genie-spaces/ba-p/107003)
- [Companion demo: Demand Forecasting & Inventory Management](https://github.com/macumberc/demand-forecasting-dbx-genie)
