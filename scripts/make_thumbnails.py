#!/usr/bin/env python
"""
Make per-bag thumbnails alongside photos.

Input tree (example):
data/02_Models/BA2037-089/photos/BA2037-089_front_01.jpeg

Output:
data/02_Models/BA2037-089/thumbnails/BA2037-089_front_01.jpeg

Rules:
- Max side length configurable (default 1600 px)
- Preserves EXIF orientation
- Skips if thumbnail exists and is up-to-date (mtime heuristic)
"""

from __future__ import annotations
import argparse, os
from pathlib import Path

from PIL import Image, ImageOps  # pip install pillow

EXTS = {".jpg", ".jpeg", ".png", ".webp"}

def make_thumb(src: Path, dst: Path, max_side: int, quality: int) -> bool:
    dst.parent.mkdir(parents=True, exist_ok=True)
    # skip if up-to-date
    if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
        return False
    with Image.open(src) as im:
        im = ImageOps.exif_transpose(im)
        w, h = im.size
        if max(w, h) > max_side:
            im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
        # choose output format
        ext = dst.suffix.lower()
        if ext in (".jpg", ".jpeg"):
            im = im.convert("RGB")
            im.save(dst, "JPEG", quality=quality, optimize=True, progressive=True)
        elif ext == ".png":
            im.save(dst, "PNG", optimize=True)
        else:
            # default to JPEG if odd extension sneaks in
            im = im.convert("RGB")
            dst = dst.with_suffix(".jpg")
            im.save(dst, "JPEG", quality=quality, optimize=True, progressive=True)
    return True

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/02_Models", help="Per-bag root")
    ap.add_argument("--max-side", type=int, default=1600, help="Max long side")
    ap.add_argument("--quality", type=int, default=88, help="JPEG quality")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    made = skipped = 0

    for photos in root.rglob("photos"):
        if not photos.is_dir(): 
            continue
        thumbs = photos.parent / "thumbnails"
        for src in photos.iterdir():
            if not (src.is_file() and src.suffix.lower() in EXTS):
                continue
            dst = thumbs / src.name
            if args.dry_run:
                if not dst.exists():
                    made += 1
                else:
                    skipped += 1
                continue
            changed = make_thumb(src, dst, args["max_side"] if isinstance(args, dict) else args.max_side, args.quality)
            made += int(changed)
            skipped += int(not changed)

    print(f"thumbnails: created={made}, up_to_date={skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
