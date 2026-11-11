#!/usr/bin/env python3
"""
Composes executive typography and tech badges onto the hero banner image.
Produces a crisp, publication-grade header for GitHub and documentation.
"""

import os
from PIL import Image, ImageDraw, ImageFont, ImageFilter

INPUT_IMAGE = "d:/LabD/Proyecto Odysseus/experiments/streaming_flink_kinesis_pipeline/docs/images/hero_streaming_pipeline.png"
OUTPUT_IMAGE = "d:/LabD/Proyecto Odysseus/experiments/streaming_flink_kinesis_pipeline/docs/images/hero_streaming_pipeline.png"

def create_banner():
    if not os.path.exists(INPUT_IMAGE):
        raise FileNotFoundError(f"Input image not found: {INPUT_IMAGE}")

    base = Image.open(INPUT_IMAGE).convert("RGBA")
    w, h = base.size  # 1376 x 768

    # 1. Create a dark gradient scrim overlay on the left/center to ensure perfect text contrast
    scrim = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    scrim_draw = ImageDraw.Draw(scrim)

    # Left-to-right soft radial/linear darkening
    for x in range(w):
        # Darkness factor: highest on left (x=0 to x=850), fades softly to the right
        if x < 850:
            alpha = int(180 * (1.0 - (x / 850.0) ** 1.3))
        else:
            alpha = 0
        scrim_draw.line([(x, 0), (x, h)], fill=(8, 12, 20, alpha))

    # Bottom scrim for badges
    for y in range(h - 180, h):
        factor = (y - (h - 180)) / 180.0
        alpha = int(140 * factor)
        scrim_draw.line([(0, y), (w, y)], fill=(8, 12, 20, alpha))

    base = Image.alpha_composite(base, scrim)
    draw = ImageDraw.Draw(base)

    # 2. Fonts
    font_bold = "C:/Windows/Fonts/bahnschrift.ttf"
    font_segoe = "C:/Windows/Fonts/segoeuib.ttf"
    font_mono = "C:/Windows/Fonts/consola.ttf"

    font_pill = ImageFont.truetype(font_mono, 20)
    font_title = ImageFont.truetype(font_bold, 54)
    font_subtitle = ImageFont.truetype(font_bold, 28)
    font_badges = ImageFont.truetype(font_mono, 18)

    start_x = 70
    start_y = 110

    # 3. Category Pill: PROYECTO ODYSSEUS • STREAMING MLOps
    pill_text = " PROYECTO ODYSSEUS • STREAMING MLOps "
    bbox = font_pill.getbbox(pill_text)
    pw = bbox[2] - bbox[0] + 30
    ph = bbox[3] - bbox[1] + 16

    # Draw pill background with border
    draw.rounded_rectangle(
        [start_x, start_y, start_x + pw, start_y + ph],
        radius=14,
        fill=(2, 44, 34, 210),        # Deep emerald
        outline=(16, 185, 129, 230),   # Vibrant emerald
        width=2
    )
    # Cyan accent dot
    draw.ellipse([start_x + 14, start_y + ph // 2 - 4, start_x + 22, start_y + ph // 2 + 4], fill=(52, 211, 153, 255))
    draw.text((start_x + 28, start_y + 6), pill_text, font=font_pill, fill=(167, 243, 208, 255))

    # 4. Main Headline: REAL-TIME STREAMING DATA PIPELINE
    title_y = start_y + ph + 28
    title_line1 = "REAL-TIME STREAMING"
    title_line2 = "DATA PIPELINE"

    # Soft glow shadow behind title
    for offset_x, offset_y in [(-2, 2), (2, 2), (0, 3), (3, 3)]:
        draw.text((start_x + offset_x, title_y + offset_y), title_line1, font=font_title, fill=(0, 0, 0, 220))
        draw.text((start_x + offset_x, title_y + 64 + offset_y), title_line2, font=font_title, fill=(0, 0, 0, 220))

    draw.text((start_x, title_y), title_line1, font=font_title, fill=(255, 255, 255, 255))
    draw.text((start_x, title_y + 64), title_line2, font=font_title, fill=(56, 189, 248, 255))  # Electric cyan

    # 5. Subtitle: Apache Flink 1.18 & AWS Kinesis
    sub_y = title_y + 145
    sub_text = "Apache Flink 1.18  •  AWS Kinesis  •  Complex Event Processing (CEP)"
    # Drop shadow
    draw.text((start_x + 2, sub_y + 2), sub_text, font=font_subtitle, fill=(0, 0, 0, 200))
    draw.text((start_x, sub_y), sub_text, font=font_subtitle, fill=(226, 232, 240, 240))

    # Description line
    desc_y = sub_y + 42
    desc_text = "Sub-50ms P99 Latency | Exactly-Once S3 Parquet | Idempotent SQS FIFO & DynamoDB"
    font_desc = ImageFont.truetype(font_mono, 19)
    draw.text((start_x, desc_y), desc_text, font=font_desc, fill=(148, 163, 184, 230))

    # 6. Tech Badge Pills along the bottom
    badges = [
        ("Java 17 (Maven)", (14, 165, 233)),       # Sky Blue
        ("RocksDB Incremental", (16, 185, 129)),   # Emerald
        ("SQS FIFO + DynamoDB", (129, 140, 248)),  # Indigo
        ("Glue Partition Projection", (245, 158, 11)) # Amber
    ]

    bx = start_x
    by = h - 85
    for label, color in badges:
        tb = font_badges.getbbox(label)
        bw = tb[2] - tb[0] + 28
        bh = tb[3] - tb[1] + 16

        # Badge background
        draw.rounded_rectangle(
            [bx, by, bx + bw, by + bh],
            radius=8,
            fill=(15, 23, 42, 220),
            outline=color + (180,),
            width=1
        )
        draw.text((bx + 14, by + 7), label, font=font_badges, fill=(241, 245, 249, 255))
        bx += bw + 14

    # Convert back to RGB and save
    final_img = base.convert("RGB")
    final_img.save(OUTPUT_IMAGE, format="PNG", quality=95)
    print(f"[SUCCESS] Banner updated with executive typography: {OUTPUT_IMAGE}")

if __name__ == "__main__":
    create_banner()
