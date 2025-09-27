#!/usr/bin/env python
"""
Migrate data/master_metadata.csv to canonical columns while preserving legacy fields.

Canonical columns:
id,filename,model,viewtype,sequence,width,height,filesize,sha256,processed_at,source,uid,sha256_manifest_path,notes

Rules:
- filename/model/viewtype/sequence are parsed from the file name if needed.
- If sha256 missing, compute from file on disk if path can be resolved.
- All unknown/legacy columns are appended into `notes` as "key=value" lines.
- Atomic rewrite with backup of the old CSV.
"""

from __future__ import annotations
from pathlib import Path
import sys, re, hashlib
import pandas as pd
from datetime import datetime, timezone

from scripts.utils.atomic_write import atomic_write_text

ROOT = Path(".").resolve()
MASTER = Path("data/master_metadata.csv")
COLS = [
    "id","filename","model","viewtype","sequence",
    "width","height","filesize","sha256","processed_at",
    "source","uid","sha256_manifest_path","notes"
]
SCHEMA = re.compile(r"^(BA\d{4}-\d{3})_([a-z]+)_(\d{2})\.[a-z0-9]+$", re.I)

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_from_filename(fn: str):
    m = SCHEMA.match(fn)
    if not m:
        return ("", "unknown", "")
    model, view, seq = m.group(1).upper(), m.group(2).lower(), m.group(3)
    return (model, view, seq)

def pick_filename(row: pd.Series) -> str:
    # Prefer explicit filename column if present
    for k in ("filename", "file_name"):
        if k in row and isinstance(row[k], str) and row[k]:
            return Path(row[k]).name
    # Try thumb_path or any path-like field
    for k in ("thumb_path","path","image_path","photo_path"):
        if k in row and isinstance(row[k], str) and row[k]:
            return Path(row[k]).name
    # Try uid/uuid if it contains a filename-looking tail
    for k in ("uid","uuid"):
        if k in row and isinstance(row[k], str) and row[k]:
            tail = Path(row[k]).name
            if "." in tail and "_" in tail:
                return tail
    return ""

def find_disk_path(model: str, filename: str) -> Path | None:
    if not (model and filename):
        return None
    # Per-bag layout path
    candidate = ROOT / f"data/02_Models/{model}/photos/{filename}"
    if candidate.exists():
        return candidate
    # Legacy data/photos layout
    candidate = ROOT / f"data/photos/{model}/{filename}"
    if candidate.exists():
        return candidate
    # Fallback: brute search (limited)
    for base in (ROOT/"data/02_Models", ROOT/"data/photos"):
        found = list(base.rglob(filename))
        if found:
            return found[0]
    return None

def legacy_to_notes(row: pd.Series, used_keys: set[str]) -> str:
    chunks = []
    for k, v in row.items():
        if k in used_keys: 
            continue
        if pd.isna(v) or v == "":
            continue
        chunks.append(f"{k}={v}")
    return "\n".join(chunks)

def main() -> int:
    if not MASTER.exists():
        print(f"ERROR: {MASTER} not found.", file=sys.stderr)
        return 2

    df_old = pd.read_csv(MASTER)
    used = {"filename","file_name","thumb_path","path","image_path","photo_path","uid","uuid"}  # temp set

    rows = []
    for _, r in df_old.iterrows():
        filename = pick_filename(r)
        model, view, seq = parse_from_filename(filename)

        # width/height/filesize if present
        width  = int(r.get("width", 0)) if pd.notna(r.get("width", 0)) else 0
        height = int(r.get("height", 0)) if pd.notna(r.get("height", 0)) else 0
        size   = int(r.get("filesize", 0)) if pd.notna(r.get("filesize", 0)) else 0

        # sha256 from row or compute from disk if missing
        sha = str(r.get("sha256")) if pd.notna(r.get("sha256")) else ""
        if not sha and filename:
            p = find_disk_path(model, filename)
            if p and p.exists():
                if size == 0:
                    try: size = p.stat().st_size
                    except Exception: pass
                try:
                    sha = sha256_file(p)
                except Exception:
                    sha = ""

        processed_at = str(r.get("processed_at")) if pd.notna(r.get("processed_at")) else datetime.now(timezone.utc).isoformat()
        source = str(r.get("source")) if pd.notna(r.get("source")) else "legacy_migration"
        uid = str(r.get("uid")) if pd.notna(r.get("uid")) else ""
        manifest = str(r.get("sha256_manifest_path")) if pd.notna(r.get("sha256_manifest_path")) else ""

        # build id
        sha8 = sha[:8] if sha else "00000000"
        seq2 = f"{int(seq):02d}" if str(seq).isdigit() else "00"
        _id = f"{model}_{view}_{seq2}_{sha8}" if model else (uid or filename or sha8)

        # Build notes from unrecognized legacy fields
        base_keys = {"id","filename","model","viewtype","sequence","width","height","filesize","sha256","processed_at","source","uid","sha256_manifest_path","notes"}
        # Collect keys we already used from legacy
        extra_used = {"width","height","filesize","sha256","processed_at","source","uid","sha256_manifest_path"}
        used_keys = base_keys | extra_used | set(used)
        notes = legacy_to_notes(r, used_keys)

        rows.append({
            "id": _id,
            "filename": filename,
            "model": model,
            "viewtype": view,
            "sequence": seq2,
            "width": width,
            "height": height,
            "filesize": size,
            "sha256": sha,
            "processed_at": processed_at,
            "source": source,
            "uid": uid,
            "sha256_manifest_path": manifest,
            "notes": notes,
        })

    new = pd.DataFrame(rows, columns=COLS)
    # de-dupe by sha256 if present
    if "sha256" in new.columns:
        new = new.drop_duplicates(subset=["sha256"], keep="first")
    new = new.sort_values(by=["model","viewtype","sequence","filename"], kind="stable")

    # Backup, then atomic write
    bkp = MASTER.with_suffix(".pre_migrate.bak.csv")
    try:
        MASTER.replace(bkp)
        print(f"Backed up old master → {bkp}")
    except Exception:
        print("WARN: could not back up master (still proceeding).")

    atomic_write_text(MASTER, new.to_csv(index=False))
    print(f"Migrated → {MASTER} (rows={len(new)})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
