#!/usr/bin/env python
"""
Bag Ingest MVP
Scans per-bag photo folders (default: data/02_Models/**/photos/*),
collects metadata, and updates:
  - data/checksums/<MODEL>.sha256        (idempotent append, path-safe on Windows)
  - data/master_metadata.csv             (atomic rewrite, de-duped by sha256)

Schema assumed for filenames: BA####-###_<viewtype>_<NN>.<ext>
Columns written to master_metadata.csv (in this order):
id,filename,model,viewtype,sequence,width,height,filesize,sha256,processed_at,source,uid,sha256_manifest_path,notes
"""

from __future__ import annotations
import argparse
import csv
import hashlib
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# Soft dep for dimensions (optional); script still runs without it.
try:
    from PIL import Image  # type: ignore
    HAVE_PIL = True
except Exception:
    HAVE_PIL = False

import pandas as pd

from scripts.utils.atomic_write import atomic_write_text

# -------- Config ----------------------------------------------------------------

EXTS = {".jpg", ".jpeg", ".png", ".webp"}  # extend if needed
SCHEMA = re.compile(r"^(BA\d{4}-\d{3})_([a-z]+)_(\d{2})\.(jpg|jpeg|png|webp)$", re.I)

MASTER_COLUMNS = [
    "id","filename","model","viewtype","sequence",
    "width","height","filesize","sha256","processed_at",
    "source","uid","sha256_manifest_path","notes"
]

# -------- Helpers ----------------------------------------------------------------

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
            return int(im.width), int(im.height)
    except Exception:
        return (0, 0)

def parse_schema(name: str) -> Tuple[str, str, int]:
    m = SCHEMA.match(name)
    if not m:
        return ("", "unknown", 0)
    model = m.group(1).upper()
    view  = m.group(2).lower()
    seq   = int(m.group(3))
    return (model, view, seq)

def load_existing_master(master_csv: Path) -> pd.DataFrame:
    if master_csv.exists() and master_csv.stat().st_size > 0:
        try:
            return pd.read_csv(master_csv)
        except Exception:
            # If unreadable, back it up and start fresh
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
    Writes atomically and stores repo-relative paths when possible.
    Windows-safe: resolves absolute paths before relative_to().
    """
    root = root.resolve()  # normalize
    checks_dir = root / "data" / "checksums"
    checks_dir.mkdir(parents=True, exist_ok=True)
    out = checks_dir / f"{model}.sha256"

    # Load existing hashes to keep this idempotent
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
            # not under repo root (or different drive) -> store absolute
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

  
