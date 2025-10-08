#!/usr/bin/env python
"""
Bag Ingest + Thumbnails + Provenance (MVP)

Scans per-bag photo folders (default: data/02_Models/**/photos/*),
collects metadata (including optional per-bag provenance files),
updates:
  - data/checksums/<MODEL>.sha256    (idempotent append, Windows-safe)
  - data/master_metadata.csv         (atomic rewrite, de-duped by sha256)

Optional:
  - Generate per-bag thumbnails in each bag's thumbnails/ folder.

Per-bag metadata files supported (place next to photos/, i.e. bag dir):
  - bag_metadata.csv (preferred) with columns: filename,listing_url,seller_id,capture_date
  - metadata.csv / metadata.json / metadata.yaml / metadata.yml

Canonical master columns (including provenance):
id,filename,model,viewtype,sequence,width,height,filesize,sha256,processed_at,
source,uid,sha256_manifest_path,listing_url,seller_id,capture_date,notes
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Pillow (optional; only needed for dims/thumbs)
try:
    from PIL import Image, ImageOps  # type: ignore
    HAVE_PIL = True
except Exception:
    HAVE_PIL = False

import pandas as pd

from scripts.utils.atomic_write import atomic_write_text
from scripts.config import load_config, pick  # config.yaml loader

# ----------------------- Defaults (overridable via config.yaml) ----------------

EXTS = {".jpg", ".jpeg", ".png", ".webp"}
SCHEMA = re.compile(r"^(BA\d{4}-\d{3})_([a-z]+)_(\d{2})\.(jpg|jpeg|png|webp)$", re.I)

MASTER_COLUMNS = [
    "id", "filename", "model", "viewtype", "sequence",
    "width", "height", "filesize", "sha256", "processed_at",
    "source", "uid", "sha256_manifest_path",
    # provenance fields (optional)
    "listing_url", "seller_id", "capture_date",
    "notes",
]

# --------------------------------- Helpers ------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def get_dims(path: Path) -> Tuple[int, int]:
    if not HAVE_PIL:
        return (0, 0)
    try:
        with Image.open(path) as im:
            im = ImageOps.exif_transpose(im)
            return int(im.width), int(im.height)
    except Exception:
        return (0, 0)


def parse_schema(name: str) -> Tuple[str, str, int]:
    m = SCHEMA.match(name)
    if not m:
        return ("", "unknown", 0)
    model = m.group(1).upper()
    view = m.group(2).lower()
    seq = int(m.group(3))
    return (model, view, seq)


def load_existing_master(master_csv: Path) -> pd.DataFrame:
    if master_csv.exists() and master_csv.stat().st_size > 0:
        try:
            return pd.read_csv(master_csv)
        except Exception:
            bkp = master_csv.with_suffix(".corrupt.bak")
            try:
                master_csv.replace(bkp)
            except Exception:
                pass
            return pd.DataFrame(columns=MASTER_COLUMNS)
    return pd.DataFrame(columns=MASTER_COLUMNS)


def ensure_checksums_append(model: str, root: Path, files_with_hashes: List[Tuple[Path, str]]) -> str:
    """
    Append unique (sha256, path) lines to data/checksums/<MODEL>.sha256.
    Windows-safe: resolves absolute paths before relative_to().
    """
    root = root.resolve()
    checks_dir = root / "data" / "checksums"
    checks_dir.mkdir(parents=True, exist_ok=True)
    out = checks_dir / f"{model}.sha256"

    existing = set()
    if out.exists():
        with out.open("r", encoding="utf-8", newline="") as f:
            for line in f:
                parts = line.strip().split(maxsplit=1)
                if parts:
                    existing.add(parts[0])

    rows: List[str] = []
    for p, h in files_with_hashes:
        if h in existing:
            continue
        p_abs = p.resolve()
        try:
            rel_txt = p_abs.relative_to(root).as_posix()
        except ValueError:
            rel_txt = p_abs.as_posix()
        rows.append(f"{h}  {rel_txt}\n")

    if rows:
        tmp = out.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as f:
            f.writelines(rows)

        if out.exists():
            merged = out.with_suffix(".merged")
            with merged.open("w", encoding="utf-8", newline="") as m:
                with out.open("r", encoding="utf-8", newline="") as old:
                    m.write(old.read())
                with tmp.open("r", encoding="utf-8", newline="") as newp:
                    m.write(newp.read())
            merged.replace(out)
            try:
                tmp.unlink()
            except Exception:
                pass
        else:
            tmp.replace(out)

    return out.as_posix()


def make_thumb(src: Path, dst: Path, max_side: int, quality: int) -> bool:
    """
    Create/refresh thumbnail for `src` at `dst`.
    Skips if dst is already up-to-date (mtime check).
    Returns True if a thumbnail was created/updated.
    """
    if not HAVE_PIL:
        return False

    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            return False
    except Exception:
        pass

    with Image.open(src) as im:
        im = ImageOps.exif_transpose(im)
        w, h = im.size
        if max(w, h) > max_side:
            im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)

        ext = dst.suffix.lower()
        if ext in (".jpg", ".jpeg"):
            im = im.convert("RGB")
            im.save(dst, "JPEG", quality=quality, optimize=True, progressive=True)
        elif ext == ".png":
            im.save(dst, "PNG", optimize=True)
        else:
            im = im.convert("RGB")
            dst = dst.with_suffix(".jpg")
            im.save(dst, "JPEG", quality=quality, optimize=True, progressive=True)
    return True


# ---------------------------- Per-bag metadata loader --------------------------


def load_bag_metadata(bag_dir: Path) -> Dict[str, Dict[str, str]]:
    """
    Look for a per-bag metadata file next to photos/:
      - bag_metadata.csv  (preferred)
      - metadata.csv
      - metadata.json
      - metadata.yaml / metadata.yml

    Returns a map: filename -> { 'listing_url': ..., 'seller_id': ..., 'capture_date': ... }
    """
    candidates = [
        bag_dir / "bag_metadata.csv",
        bag_dir / "metadata.csv",
        bag_dir / "metadata.json",
        bag_dir / "metadata.yaml",
        bag_dir / "metadata.yml",
    ]
    meta_map: Dict[str, Dict[str, str]] = {}
    for c in candidates:
        if not c.exists():
            continue
        try:
            if c.suffix.lower() == ".csv":
                import pandas as _pd
                df = _pd.read_csv(c, dtype=str).fillna("")
                for _, row in df.iterrows():
                    fn = str(row.get("filename", "") or "")
                    if not fn:
                        for k in ("file_name", "path", "image_path"):
                            if k in df.columns and row.get(k):
                                fn = Path(str(row.get(k))).name
                                break
                    if not fn:
                        continue
                    meta_map[Path(fn).name] = {
                        "listing_url": str(row.get("listing_url", "") or ""),
                        "seller_id": str(row.get("seller_id", "") or ""),
                        "capture_date": str(row.get("capture_date", "") or ""),
                    }
                return meta_map
            elif c.suffix.lower() == ".json":
                data = json.loads(c.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    for k, v in data.items():
                        fn = Path(k).name
                        meta_map[fn] = {
                            "listing_url": str(v.get("listing_url", "") or ""),
                            "seller_id": str(v.get("seller_id", "") or ""),
                            "capture_date": str(v.get("capture_date", "") or ""),
                        }
                elif isinstance(data, list):
                    for item in data:
                        fn = Path(str(item.get("filename", "") or "")).name
                        if not fn:
                            continue
                        meta_map[fn] = {
                            "listing_url": str(item.get("listing_url", "") or ""),
                            "seller_id": str(item.get("seller_id", "") or ""),
                            "capture_date": str(item.get("capture_date", "") or ""),
                        }
                return meta_map
            elif c.suffix.lower() in (".yaml", ".yml"):
                try:
                    import yaml as _yaml
                except Exception:
                    continue
                data = _yaml.safe_load(c.read_text(encoding="utf-8")) or {}
                if isinstance(data, dict):
                    if all(isinstance(v, dict) for v in data.values()):
                        for k, v in data.items():
                            fn = Path(k).name
                            meta_map[fn] = {
                                "listing_url": str(v.get("listing_url", "") or ""),
                                "seller_id": str(v.get("seller_id", "") or ""),
                                "capture_date": str(v.get("capture_date", "") or ""),
                            }
                        return meta_map
                    # else try list-like shape
                    iterable = data if isinstance(data, list) else list(data.values())
                    for item in iterable:
                        fn = Path(str(item.get("filename", "") or "")).name
                        if not fn:
                            continue
                        meta_map[fn] = {
                            "listing_url": str(item.get("listing_url", "") or ""),
                            "seller_id": str(item.get("seller_id", "") or ""),
                            "capture_date": str(item.get("capture_date", "") or ""),
                        }
                    return meta_map
        except Exception:
            continue
    return meta_map


# ----------------------------------- Main -------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/02_Models", help="Per-bag root (BAxxxx-xxx/photos)")
    ap.add_argument("--master", default="data/master_metadata.csv", help="Master metadata CSV")
    ap.add_argument("--dry-run", action="store_true", help="Preview only; no writes")
    ap.add_argument("--source", default="local_ingest", help="Source label for provenance")
    # thumbnails
    ap.add_argument("--thumbs", action="store_true", help="Also generate thumbnails per bag")
    ap.add_argument("--thumb-max-side", type=int, default=1600, help="Thumbnail max long side")
    ap.add_argument("--thumb-quality", type=int, default=88, help="Thumbnail JPEG quality")
    # config
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = ap.parse_args()

    # ----- load config (CLI still wins over defaults) -----
    cfg = load_config(args.config)

    if args.root == ap.get_default("root"):
        v = pick(cfg, "roots", "models", default=None)
        if v:
            args.root = v

    if args.master == ap.get_default("master"):
        v = pick(cfg, "master", default=None)
        if v:
            args.master = v

    if args.thumbs is False:
        v = pick(cfg, "thumbs", "enabled", default=None)
        if isinstance(v, bool):
            args.thumbs = v

    if args.thumb_max_side == ap.get_default("thumb_max_side"):
        v = pick(cfg, "thumbs", "max_side", default=None)
        if isinstance(v, int):
            args.thumb_max_side = v

    if args.thumb_quality == ap.get_default("thumb_quality"):
        v = pick(cfg, "thumbs", "quality", default=None)
        if isinstance(v, int):
            args.thumb_quality = v

    exts_cfg = pick(cfg, "exts", default=None)
    if isinstance(exts_cfg, list) and exts_cfg:
        global EXTS  # update allowed extensions set (module-level)
        EXTS = {str(x).lower() for x in exts_cfg}

    repo_root = Path(".").resolve()
    root = Path(args.root)
    master_csv = Path(args.master)

    if not root.exists():
        print(f"ERROR: root not found: {root}", file=sys.stderr)
        return 2

    existing = load_existing_master(master_csv)
    for c in MASTER_COLUMNS:
        if c not in existing.columns:
            existing[c] = ""

    # Scan photos and collect new rows
    new_rows: List[Dict[str, object]] = []
    file_map: Dict[str, Path] = {}

    for photos_dir in root.rglob("photos"):
        if not photos_dir.is_dir():
            continue

        # load optional per-bag metadata (search next to photos/ i.e., bag_dir/)
        bag_dir = photos_dir.parent
        bag_meta = load_bag_metadata(bag_dir)  # filename -> {listing_url, seller_id, capture_date}

        for p in photos_dir.iterdir():
            if not (p.is_file() and p.suffix.lower() in EXTS):
                continue

            file_map[p.name] = p
            model, view, seq = parse_schema(p.name)
            if not model:
                # non-conforming names should have been normalized already
                continue

            sha = sha256_file(p)
            # skip if sha already present
            if "sha256" in existing.columns and (existing["sha256"] == sha).any():
                continue

            width, height = get_dims(p)
            size = p.stat().st_size
            processed = datetime.now(timezone.utc).isoformat()
            sha8 = sha[:8]
            _id = f"{model}_{view}_{seq:02d}_{sha8}"

            # per-file provenance if available
            file_meta = bag_meta.get(p.name, {})
            listing_url = file_meta.get("listing_url", "")
            seller_id = file_meta.get("seller_id", "")
            capture_date = file_meta.get("capture_date", "")

            new_rows.append({
                "id": _id,
                "filename": p.name,
                "model": model,
                "viewtype": view,
                "sequence": f"{seq:02d}",
                "width": width,
                "height": height,
                "filesize": size,
                "sha256": sha,
                "processed_at": processed,
                "source": args.source,
                "uid": "",
                "sha256_manifest_path": "",
                "listing_url": listing_url,
                "seller_id": seller_id,
                "capture_date": capture_date,
                "notes": "",
            })

    if not new_rows:
        print("No new rows to ingest. ✅")
        # optionally refresh/create thumbnails even if nothing new
        if args.thumbs:
            made = skipped = 0
            for photos_dir in root.rglob("photos"):
                if not photos_dir.is_dir():
                    continue
                thumbs_dir = photos_dir.parent / "thumbnails"
                for p in photos_dir.iterdir():
                    if not (p.is_file() and p.suffix.lower() in EXTS):
                        continue
                    dst = thumbs_dir / p.name
                    if make_thumb(p, dst, args.thumb_max_side, args.thumb_quality):
                        made += 1
                    else:
                        skipped += 1
            print(f"thumbnails: created={made}, up_to_date={skipped}")
        return 0

    # Group by model for checksum manifests
    by_model: Dict[str, List[Tuple[Path, str]]] = {}
    for r in new_rows:
        p = file_map.get(r["filename"])  # type: ignore[arg-type]
        if p is None:
            continue
        by_model.setdefault(r["model"], []).append((p, r["sha256"]))  # type: ignore[index]

    # Write/append per-model checksum manifests
    manifest_by_model: Dict[str, str] = {}
    for model, pairs in by_model.items():
        manifest_path = ensure_checksums_append(model, repo_root, pairs)
        manifest_by_model[model] = manifest_path

    # Fill manifest path on rows
    for r in new_rows:
        r["sha256_manifest_path"] = manifest_by_model.get(r["model"], "")

    # Merge with existing and de-dup by sha256
    new_df = pd.DataFrame(new_rows, columns=MASTER_COLUMNS)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["sha256"], keep="first")

    # Stable sort for pleasant diffs
    for c in ("model", "viewtype", "sequence", "filename"):
        if c not in combined.columns:
            combined[c] = ""
    combined = combined.sort_values(by=["model", "viewtype", "sequence", "filename"], kind="stable")

    out_csv = combined.to_csv(index=False)

    if args.dry_run:
        print(f"[DRY] Would add {len(new_rows)} new rows; master would become {len(combined)} rows.")
        return 0

    atomic_write_text(master_csv, out_csv)
    print(f"Ingested {len(new_rows)} new rows → {master_csv}")

    # Optional thumbnails after ingest
    if args.thumbs:
        made = skipped = 0
        for photos_dir in root.rglob("photos"):
            if not photos_dir.is_dir():
                continue
            thumbs_dir = photos_dir.parent / "thumbnails"
            for p in photos_dir.iterdir():
                if not (p.is_file() and p.suffix.lower() in EXTS):
                    continue
                dst = thumbs_dir / p.name
                if make_thumb(p, dst, args.thumb_max_side, args.thumb_quality):
                    made += 1
                else:
                    skipped += 1
        print(f"thumbnails: created={made}, up_to_date={skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
