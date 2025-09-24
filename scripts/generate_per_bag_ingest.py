#!/usr/bin/env python3
"""
generate_per_bag_ingest.py

Usage examples:
  # dry-run, list what would happen
  python scripts/generate_per_bag_ingest.py --repo "C:\\Users\\tamir\\Documents\\Nike SB RPM Master Archive" --bag BA2037-089 --in "C:\\temp\\bag_photos" --dry-run

  # actually resize images (max dimension 2048px) and update master metadata
  python scripts/generate_per_bag_ingest.py --repo /path/to/repo --bag BA2037-089 --in ./incoming/BA2037-089 --mode resize --max-dim 2048

  # just copy files into canonical output without resizing
  python scripts/generate_per_bag_ingest.py --repo /path/to/repo --bag BA2037-089 --in ./incoming/BA2037-089 --mode copy
"""
import os
import re
import argparse
import shutil
import hashlib
import csv
import logging
from datetime import datetime
from pathlib import Path

try:
    from PIL import Image, ExifTags
except Exception as e:
    raise RuntimeError("This script requires Pillow. Install with: pip install Pillow") from e

# -----------------------
# Config / defaults
# -----------------------
FILENAME_RE = re.compile(r'^BA\d{4}-\d{3}_[a-z0-9_-]+_\d+\.(jpe?g|png)$', re.IGNORECASE)
MASTER_CSV_HEADER = [
    "id", "filename", "model", "viewtype", "sequence",
    "width", "height", "filesize", "sha256", "processed_at", "notes"
]

# -----------------------
# Helpers
# -----------------------
def compute_sha256(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()

def parse_filename(fname):
    """
    Returns (model, viewtype, sequence) or None if not match
    filename expected: BA####-###_[view]_[seq].ext
    """
    base = os.path.basename(fname)
    if not FILENAME_RE.match(base):
        return None
    name, _ext = os.path.splitext(base)
    parts = name.split("_")
    if len(parts) < 3:
        return None
    model = parts[0]
    viewtype = "_".join(parts[1:-1])
    sequence = parts[-1]
    return model, viewtype, sequence

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
    return p

def resize_image(in_path, out_path, max_dim):
    with Image.open(in_path) as im:
        # preserve orientation (EXIF) when present
        try:
            exif = im._getexif() or {}
            orientation_key = next((k for k, v in ExifTags.TAGS.items() if v == 'Orientation'), None)
            if orientation_key and orientation_key in exif:
                orientation = exif[orientation_key]
                if orientation == 3:
                    im = im.rotate(180, expand=True)
                elif orientation == 6:
                    im = im.rotate(270, expand=True)
                elif orientation == 8:
                    im = im.rotate(90, expand=True)
        except Exception:
            pass

        w, h = im.size
        max_current = max(w, h)
        if max_current <= max_dim:
            # no resize needed; still save to out_path to normalize metadata/copy
            im.save(out_path, quality=95)
            return im.size
        scale = max_dim / float(max_current)
        new_w = int(w * scale)
        new_h = int(h * scale)
        im_resized = im.resize((new_w, new_h), Image.LANCZOS)
        im_resized.save(out_path, quality=95)
        return im_resized.size

def append_row_to_csv(csv_path, row, header):
    is_new = not os.path.exists(csv_path)
    with open(csv_path, "a", newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        if is_new:
            writer.writeheader()
        writer.writerow(row)

def load_existing_hashes(csv_path):
    hashes = set()
    if not os.path.exists(csv_path):
        return hashes
    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            if r.get("sha256"):
                hashes.add(r["sha256"])
    return hashes

# -----------------------
# Main
# -----------------------
def main(args):
    repo = Path(args.repo).expanduser().resolve()
    in_dir = Path(args.in_dir).expanduser().resolve()
    bag = args.bag
    if bag is None:
        logging.error("You must provide --bag (e.g. BA2037-089).")
        return 2

    # output layout under repo
    out_photos_dir = repo / "data" / "photos" / bag
    ensure_dir(out_photos_dir)
    master_csv = repo / "data" / "master_metadata.csv"
    skipped_csv = repo / "data" / f"skipped_filenames_{bag}.csv"
    processed_count = 0
    skipped = []
    existing_hashes = load_existing_hashes(master_csv)

    logging.info(f"Repo: {repo}")
    logging.info(f"Input folder: {in_dir}")
    logging.info(f"Output folder: {out_photos_dir}")
    logging.info(f"Master CSV: {master_csv}")

    files = sorted([p for p in in_dir.iterdir() if p.is_file()])
    logging.info(f"Found {len(files)} files in input dir.")

    for p in files:
        base = p.name
        parsed = parse_filename(base)
        if parsed is None:
            skipped.append({"filename": base, "reason": "bad_filename"})
            logging.debug(f"Skipping malformed filename: {base}")
            continue

        model, viewtype, sequence = parsed
        if model.upper() != bag.upper():
            skipped.append({"filename": base, "reason": f"model_mismatch (found {model})"})
            logging.debug(f"Skipping model mismatch: {base} (expected {bag})")
            continue

        out_path = out_photos_dir / base

        if args.mode == "resize":
            if not args.dry_run:
                width, height = resize_image(str(p), str(out_path), args.max_dim)
            else:
                # dry-run: inspect size but don't write
                with Image.open(p) as im:
                    width, height = im.size
        else:  # copy
            if not args.dry_run:
                shutil.copy2(p, out_path)
                with Image.open(out_path) as im:
                    width, height = im.size
            else:
                with Image.open(p) as im:
                    width, height = im.size

        filesize = p.stat().st_size
        sha256 = compute_sha256(str(out_path)) if (not args.dry_run and out_path.exists()) else compute_sha256(str(p))
        if sha256 in existing_hashes:
            logging.info(f"Duplicate file (sha256 exists), skipping metadata append: {base}")
            processed_count += 1
            continue

        row = {
            "id": f"{bag}_{sequence}_{base}",
            "filename": str(Path("data") / "photos" / bag / base).replace("\\", "/"),
            "model": bag,
            "viewtype": viewtype,
            "sequence": sequence,
            "width": width,
            "height": height,
            "filesize": filesize,
            "sha256": sha256,
            "processed_at": datetime.utcnow().isoformat() + "Z",
            "notes": ""
        }

        if not args.dry_run:
            append_row_to_csv(str(master_csv), row, MASTER_CSV_HEADER)
            existing_hashes.add(sha256)

        processed_count += 1

    # write skipped CSV
    if skipped:
        skipped_path = repo / "data" / f"skipped_filenames_{bag}.csv"
        with open(skipped_path, "w", newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=["filename", "reason"])
            w.writeheader()
            for s in skipped:
                w.writerow(s)
        logging.info(f"Wrote skipped filenames to {skipped_path}")

    logging.info(f"Processed {processed_count} files; skipped {len(skipped)} malformed/mismatch files.")
    if args.dry_run:
        logging.info("Dry-run mode — no files were written and master CSV was not updated.")

    return 0

# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-bag image ingest -> standardized output + master metadata.")
    parser.add_argument("--repo", required=True, help="Path to archive repo root (defaults to current dir).")
    parser.add_argument("--bag", required=True, help="Model code (e.g. BA2037-089).")
    parser.add_argument("--in-dir", required=True, help="Input folder with raw photos for this bag.")
    parser.add_argument("--mode", choices=["resize", "copy"], default="resize",
                        help="resize (normalize to max dimension) or copy raw files into output.")
    parser.add_argument("--max-dim", type=int, default=2048, help="Max width/height in pixels when resizing.")
    parser.add_argument("--dry-run", action="store_true", help="Don't write files or update master CSV; just report.")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s: %(message)s")
    raise SystemExit(main(args))
