"""Synthetic inspection image generation and UC Volume upload."""

from __future__ import annotations

import io
import hashlib
import json
import os
import urllib.error
import urllib.request
from typing import Any, Optional

from .config import (
    CAMERAS_PER_DOCK,
    CATEGORY_COLORS,
    DEFECT_TYPE_BY_INSPECTION,
    HTTP_TIMEOUT_SECONDS,
    INSPECTION_TYPES,
    VOLUME_NAME,
)

_IMAGE_WIDTH = 640
_IMAGE_HEIGHT = 480


def generate_and_upload_images(
    spark: Any,
    fqn: str,
    seed: int,
    log_fn=print,
) -> tuple[str, int]:
    """Generate synthetic inspection images and upload to a UC Volume.

    Returns (volume_path, image_count).
    """
    from PIL import Image, ImageDraw, ImageFont  # noqa: E402

    catalog, schema = fqn.split(".", 1)
    volume_path = f"/Volumes/{catalog}/{schema}/{VOLUME_NAME}"
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {fqn}.{VOLUME_NAME} "
        f"COMMENT 'Synthetic inspection images for automated QC demo'"
    )
    log_fn(f"  Volume ready: {volume_path}")

    image_specs = _build_image_specs(seed)
    log_fn(f"  Generating {len(image_specs)} inspection images ...")

    count = 0
    for spec in image_specs:
        img_bytes = _render_image(spec, seed)
        _upload_to_volume(spark, volume_path, spec["filename"], img_bytes)
        count += 1

    log_fn(f"  Uploaded {count} images to {volume_path}")
    return volume_path, count


def _build_image_specs(seed: int) -> list[dict[str, Any]]:
    """Build the matrix of images to generate."""
    specs = []
    categories = list(CATEGORY_COLORS.keys())

    for category in categories:
        cat_slug = category.lower().replace(" & ", "_")
        for itype in INSPECTION_TYPES:
            specs.append({
                "category": category,
                "inspection_type": itype,
                "result": "pass",
                "defect_type": None,
                "filename": f"{cat_slug}_{itype}_pass.png",
            })

            defect_types = DEFECT_TYPE_BY_INSPECTION.get(itype, ())
            for defect in defect_types:
                specs.append({
                    "category": category,
                    "inspection_type": itype,
                    "result": "fail",
                    "defect_type": defect,
                    "filename": f"{cat_slug}_{itype}_{defect}.png",
                })

    return specs


def _render_image(spec: dict[str, Any], seed: int) -> bytes:
    """Render a single synthetic inspection image as PNG bytes."""
    from PIL import Image, ImageDraw, ImageFont

    cat_color = CATEGORY_COLORS.get(spec["category"], (180, 180, 180))
    is_pass = spec["result"] == "pass"
    defect = spec["defect_type"]

    img = Image.new("RGB", (_IMAGE_WIDTH, _IMAGE_HEIGHT), (240, 240, 240))
    draw = ImageDraw.Draw(img)

    try:
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
        font_med = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except (OSError, IOError):
        font_large = ImageFont.load_default()
        font_med = font_large
        font_small = font_large

    _draw_box(draw, cat_color)
    _draw_label(draw, spec, font_med, font_small)
    _draw_header(draw, spec, font_large, is_pass)

    if not is_pass and defect:
        _draw_defect_overlay(draw, img, defect, seed, spec)

    _draw_camera_hud(draw, spec, font_small)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _draw_box(draw: Any, color: tuple[int, int, int]) -> None:
    """Draw a shipping box in the center of the image."""
    box_left, box_top = 120, 80
    box_right, box_bottom = 520, 380

    shadow_offset = 6
    draw.rectangle(
        [box_left + shadow_offset, box_top + shadow_offset,
         box_right + shadow_offset, box_bottom + shadow_offset],
        fill=(200, 200, 200),
    )

    draw.rectangle([box_left, box_top, box_right, box_bottom], fill=(245, 222, 179), outline=(139, 119, 101), width=3)

    flap_mid = (box_left + box_right) // 2
    flap_top = box_top - 25
    draw.polygon(
        [(box_left, box_top), (flap_mid, flap_top), (box_right, box_top)],
        fill=(222, 196, 154),
        outline=(139, 119, 101),
    )

    tape_width = 30
    draw.rectangle(
        [flap_mid - tape_width // 2, box_top - 10,
         flap_mid + tape_width // 2, box_top + 40],
        fill=(200, 180, 140),
        outline=(170, 150, 110),
    )

    stripe_y = box_bottom - 30
    draw.rectangle([box_left + 10, stripe_y, box_right - 10, stripe_y + 8], fill=color)


def _draw_label(draw: Any, spec: dict, font_med: Any, font_small: Any) -> None:
    """Draw the product label on the box."""
    label_left, label_top = 200, 140
    label_right, label_bottom = 440, 280

    draw.rectangle([label_left, label_top, label_right, label_bottom], fill=(255, 255, 255), outline=(100, 100, 100), width=2)

    draw.text((label_left + 10, label_top + 8), "NorthStar Logistics", fill=(0, 0, 0), font=font_med)
    draw.text((label_left + 10, label_top + 35), spec["category"], fill=(80, 80, 80), font=font_small)

    barcode_y = label_top + 60
    bar_x = label_left + 20
    h = hashlib.md5(spec["filename"].encode()).hexdigest()
    for i, ch in enumerate(h[:28]):
        width = 2 if int(ch, 16) % 3 == 0 else 4 if int(ch, 16) % 3 == 1 else 3
        if int(ch, 16) % 2 == 0:
            draw.rectangle([bar_x, barcode_y, bar_x + width, barcode_y + 50], fill=(0, 0, 0))
        bar_x += width + 2

    draw.text((label_left + 10, label_bottom - 22), spec["inspection_type"].replace("_", " ").upper(), fill=(120, 120, 120), font=font_small)


def _draw_header(draw: Any, spec: dict, font: Any, is_pass: bool) -> None:
    """Draw the status banner at the top."""
    if is_pass:
        color = (34, 139, 34)
        text = "PASS"
    else:
        color = (220, 20, 60)
        text = "FAIL"

    draw.rectangle([0, 0, _IMAGE_WIDTH, 45], fill=color)
    draw.text((20, 10), f"QC INSPECTION: {text}", fill=(255, 255, 255), font=font)

    itype_text = spec["inspection_type"].replace("_", " ").title()
    draw.text((_IMAGE_WIDTH - 220, 10), itype_text, fill=(255, 255, 255), font=font)


def _draw_defect_overlay(draw: Any, img: Any, defect: str, seed: int, spec: dict) -> None:
    """Draw defect-specific visual overlay."""
    from PIL import Image as PILImage

    overlay = PILImage.new("RGBA", img.size, (0, 0, 0, 0))
    ov_draw = ImageDraw_on(overlay)

    if defect == "damaged_box":
        for i in range(5):
            x = 150 + i * 60
            y = 120 + (i % 3) * 50
            ov_draw.ellipse([x, y, x + 40 + i * 5, y + 25], fill=(180, 30, 30, 100))
        for i in range(3):
            x1 = 180 + i * 80
            y1 = 150 + i * 30
            x2 = x1 + 50
            y2 = y1 + 3
            ov_draw.rectangle([x1, y1, x2, y2], fill=(120, 20, 20, 140))

    elif defect == "torn_label":
        label_cx, label_cy = 320, 210
        for i in range(8):
            x = label_cx - 60 + i * 15
            y = label_cy - 20 + (i % 3) * 20
            ov_draw.line([(x, y), (x + 20, y + 15)], fill=(200, 50, 50, 160), width=3)
        ov_draw.rectangle([260, 190, 380, 230], fill=(200, 200, 200, 100))

    elif defect == "sku_mismatch":
        ov_draw.line([(200, 140), (440, 280)], fill=(255, 0, 0, 180), width=5)
        ov_draw.line([(440, 140), (200, 280)], fill=(255, 0, 0, 180), width=5)
        try:
            stamp_font = ImageFont_truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        except (OSError, IOError):
            stamp_font = ImageFont_load_default()
        ov_draw.text((220, 290), "SKU MISMATCH", fill=(255, 0, 0, 200), font=stamp_font)

    elif defect == "count_discrepancy":
        try:
            count_font = ImageFont_truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        except (OSError, IOError):
            count_font = ImageFont_load_default()
        ov_draw.rounded_rectangle([140, 390, 500, 440], radius=8, fill=(255, 165, 0, 160))
        ov_draw.text((155, 398), "COUNT: Expected 48 | Found 43", fill=(0, 0, 0, 220), font=count_font)

    elif defect == "crushed_corner":
        points = [(460, 320), (520, 320), (520, 380), (490, 380), (460, 350)]
        ov_draw.polygon(points, fill=(160, 82, 45, 120))
        for i in range(4):
            ov_draw.line(
                [(465 + i * 8, 325 + i * 5), (510 - i * 5, 370 - i * 8)],
                fill=(100, 50, 20, 160), width=2,
            )

    elif defect == "water_damage":
        for i in range(12):
            x = 160 + (i * 37) % 300
            y = 200 + (i * 23) % 150
            r = 15 + (i * 7) % 25
            ov_draw.ellipse([x, y, x + r, y + r], fill=(70, 130, 180, 60))
        ov_draw.rectangle([130, 350, 510, 375], fill=(70, 130, 180, 40))

    elif defect == "missing_label":
        ov_draw.rectangle([200, 140, 440, 280], fill=(220, 220, 220, 180))
        try:
            ml_font = ImageFont_truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        except (OSError, IOError):
            ml_font = ImageFont_load_default()
        ov_draw.text((245, 195), "NO LABEL FOUND", fill=(180, 0, 0, 200), font=ml_font)

    elif defect == "dented_container":
        ov_draw.ellipse([280, 180, 380, 260], fill=(180, 160, 120, 80))
        for i in range(6):
            angle_x = 300 + i * 12
            angle_y = 200 + (i % 3) * 15
            ov_draw.arc(
                [angle_x, angle_y, angle_x + 30, angle_y + 20],
                start=0, end=180, fill=(120, 100, 70, 140), width=2,
            )

    img.paste(PILImage.alpha_composite(
        img.convert("RGBA"), overlay
    ).convert("RGB"))


def ImageDraw_on(img: Any) -> Any:
    """Get an ImageDraw instance."""
    from PIL import ImageDraw
    return ImageDraw.Draw(img)


def ImageFont_truetype(path: str, size: int) -> Any:
    from PIL import ImageFont
    return ImageFont.truetype(path, size)


def ImageFont_load_default() -> Any:
    from PIL import ImageFont
    return ImageFont.load_default()


def _draw_camera_hud(draw: Any, spec: dict, font: Any) -> None:
    """Draw the camera HUD overlay at the bottom."""
    draw.rectangle([0, _IMAGE_HEIGHT - 35, _IMAGE_WIDTH, _IMAGE_HEIGHT], fill=(0, 0, 0))
    cam_text = f"CAM-01 | {spec['inspection_type'].replace('_', ' ').upper()} | {spec['category']}"
    draw.text((10, _IMAGE_HEIGHT - 28), cam_text, fill=(0, 255, 0), font=font)

    status = "PASS" if spec["result"] == "pass" else "DEFECT DETECTED"
    color = (0, 255, 0) if spec["result"] == "pass" else (255, 60, 60)
    draw.text((_IMAGE_WIDTH - 180, _IMAGE_HEIGHT - 28), status, fill=color, font=font)


def _upload_to_volume(spark: Any, volume_path: str, filename: str, data: bytes) -> None:
    """Upload bytes to a Unity Catalog Volume via the Files API."""
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = _api_token(spark)

    uc_path = f"{volume_path}/{filename}"
    api_path = f"/api/2.0/fs/files{uc_path}"

    request = urllib.request.Request(
        url=f"https://{workspace_url}{api_path}",
        data=data,
        method="PUT",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            pass
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"PUT {api_path} failed ({exc.code}): {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"PUT {api_path} failed: {exc}") from exc


def _api_token(spark: Any) -> str:
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


def _get_dbutils(spark: Any) -> Any:
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
