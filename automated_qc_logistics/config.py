"""Package-wide configuration for the automated QC logistics demo."""

from __future__ import annotations

PACKAGE_NAME = "automated-qc-logistics"
PACKAGE_VERSION = "1.0.0"

DEFAULT_SEED = 20250309
DEFAULT_SCHEMA_BASENAME = "automated_qc"
DEFAULT_SCHEMA_COMMENT = (
    "NorthStar Logistics computer-vision automated quality control demo data"
)

DEFAULT_SPACE_TITLE = "NorthStar Logistics \u2014 Automated QC \U0001f4f7"
SPACE_DESCRIPTION_MARKER = "Managed by automated_qc_logistics"

AUTO_WAREHOUSE = "auto"
HTTP_TIMEOUT_SECONDS = 30

INSPECTION_TABLE = "inspection_events"
DOCK_SCANS_TABLE = "dock_scans"
MODEL_PERF_TABLE = "model_performance"

TABLE_NAMES = (
    INSPECTION_TABLE,
    DOCK_SCANS_TABLE,
    MODEL_PERF_TABLE,
)

VOLUME_NAME = "inspection_images"

PRODUCTS = [
    {"sku": "SKU-EL-001", "name": "Wireless Bluetooth Speaker", "category": "Electronics", "price": 49.99, "unit_cost": 28.50},
    {"sku": "SKU-EL-003", "name": "USB-C Hub 7-in-1", "category": "Electronics", "price": 39.99, "unit_cost": 22.00},
    {"sku": "SKU-EL-007", "name": "Portable Phone Charger 20000mAh", "category": "Electronics", "price": 29.99, "unit_cost": 16.50},
    {"sku": "SKU-EL-010", "name": "Noise-Cancelling Earbuds", "category": "Electronics", "price": 79.99, "unit_cost": 42.00},
    {"sku": "SKU-EL-015", "name": "4K Webcam with Ring Light", "category": "Electronics", "price": 64.99, "unit_cost": 35.00},
    {"sku": "SKU-AP-003", "name": "Cold Brew Coffee Concentrate", "category": "Food & Beverage", "price": 14.99, "unit_cost": 7.80},
    {"sku": "SKU-AP-012", "name": "Organic Protein Bars (24pk)", "category": "Food & Beverage", "price": 32.50, "unit_cost": 18.75},
    {"sku": "SKU-AP-018", "name": "Sparkling Water Variety Pack", "category": "Food & Beverage", "price": 22.99, "unit_cost": 12.50},
    {"sku": "SKU-AP-025", "name": "Premium Trail Mix (12pk)", "category": "Food & Beverage", "price": 27.99, "unit_cost": 15.00},
    {"sku": "SKU-HG-002", "name": "Stainless Steel Water Bottle", "category": "Home & Garden", "price": 22.50, "unit_cost": 12.30},
    {"sku": "SKU-HG-005", "name": "Smart LED Bulb (4-pack)", "category": "Home & Garden", "price": 24.99, "unit_cost": 14.20},
    {"sku": "SKU-HG-009", "name": "Bamboo Cutting Board Set", "category": "Home & Garden", "price": 34.99, "unit_cost": 18.50},
    {"sku": "SKU-HG-014", "name": "Indoor Herb Garden Kit", "category": "Home & Garden", "price": 42.99, "unit_cost": 22.00},
    {"sku": "SKU-HL-008", "name": "Vitamin D3 Supplements", "category": "Health & Wellness", "price": 18.99, "unit_cost": 8.50},
    {"sku": "SKU-HL-015", "name": "Melatonin Sleep Gummies", "category": "Health & Wellness", "price": 12.99, "unit_cost": 6.20},
    {"sku": "SKU-HL-022", "name": "Probiotic Capsules (60ct)", "category": "Health & Wellness", "price": 24.99, "unit_cost": 12.50},
    {"sku": "SKU-HL-030", "name": "Collagen Powder (30 servings)", "category": "Health & Wellness", "price": 35.99, "unit_cost": 19.00},
    {"sku": "SKU-CL-004", "name": "Moisture-Wicking Running Socks", "category": "Clothing & Apparel", "price": 8.99, "unit_cost": 4.10},
    {"sku": "SKU-CL-011", "name": "Performance Compression Tights", "category": "Clothing & Apparel", "price": 44.99, "unit_cost": 24.00},
    {"sku": "SKU-CL-019", "name": "Quick-Dry Hiking Shorts", "category": "Clothing & Apparel", "price": 38.99, "unit_cost": 20.00},
]

WAREHOUSES = [
    {"warehouse_id": "WH-EAST-01", "warehouse_name": "Newark DC", "region": "Northeast"},
    {"warehouse_id": "WH-EAST-02", "warehouse_name": "Atlanta DC", "region": "Southeast"},
    {"warehouse_id": "WH-CENT-01", "warehouse_name": "Chicago DC", "region": "Midwest"},
    {"warehouse_id": "WH-CENT-02", "warehouse_name": "Minneapolis DC", "region": "Great Lakes"},
    {"warehouse_id": "WH-SOUTH-01", "warehouse_name": "Dallas DC", "region": "South Central"},
    {"warehouse_id": "WH-SOUTH-02", "warehouse_name": "Miami DC", "region": "Southeast"},
    {"warehouse_id": "WH-WEST-01", "warehouse_name": "Los Angeles DC", "region": "Pacific"},
    {"warehouse_id": "WH-WEST-02", "warehouse_name": "Denver DC", "region": "Mountain"},
]

CARRIERS = [
    "FedEx Freight",
    "UPS Supply Chain",
    "XPO Logistics",
    "Old Dominion",
    "SAIA",
    "Estes Express",
]

DOCKS_PER_FACILITY = 4
CAMERAS_PER_DOCK = 3

INSPECTION_TYPES = ("item_count", "label_scan", "damage_check")

DEFECT_TYPES = (
    "damaged_box",
    "torn_label",
    "sku_mismatch",
    "count_discrepancy",
    "crushed_corner",
    "water_damage",
    "missing_label",
    "dented_container",
)

DEFECT_TYPE_BY_INSPECTION = {
    "item_count": ("count_discrepancy",),
    "label_scan": ("torn_label", "sku_mismatch", "missing_label"),
    "damage_check": ("damaged_box", "crushed_corner", "water_damage", "dented_container"),
}

SEVERITIES = ("critical", "major", "minor")

MODEL_NAMES = {
    "item_count": "item_counter",
    "label_scan": "label_reader",
    "damage_check": "damage_detector",
}

MODEL_VERSIONS = {
    "item_counter": ("v2.0", "v2.1", "v2.2"),
    "label_reader": ("v3.0", "v3.1", "v3.2"),
    "damage_detector": ("v4.0", "v4.1", "v4.2"),
}

CATEGORY_COLORS = {
    "Electronics": (70, 130, 180),
    "Food & Beverage": (60, 179, 113),
    "Home & Garden": (210, 180, 140),
    "Health & Wellness": (147, 112, 219),
    "Clothing & Apparel": (240, 128, 128),
}
