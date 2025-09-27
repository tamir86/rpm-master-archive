#!/usr/bin/env python
"""
De-duplicate and sort data/master_metadata_simple.csv with an atomic rewrite.
- Drops rows with empty filename
- Drops duplicate (filename, sha256) if both columns exist; else by filename
- Sorts by (model, viewtype, sequence) if present
"""

from pathlib import Path
import sys

# Lazy import so we can give a helpful message if pandas isn't installed
try:
    import pandas as pd
except ImportError:
    sys.stderr.write("ERROR: pandas is not installed. Run: pip install pandas\n")
    sys.exit(1)

from scripts.utils.atomic_write import atomic_write_text

SRC = Path("data/master_metadata_simple.csv")

def main() -> int:
    if not SRC.exists():
        sys.stderr.write(f"NOTICE: {SRC} not found — nothing to clean.\n")
        return 0

    df = pd.read_csv(SRC)

    # keep only rows with a non-empty filename
    if "filename" in df.columns:
        df["filename"] = df["filename"].astype(str)
        df = df[df["filename"].str.len() > 0]

    # drop duplicates
    if {"filename", "sha256"} <= set(df.columns):
        df = df.drop_duplicates(subset=["filename", "sha256"]).copy()
    elif "filename" in df.columns:
        df = df.drop_duplicates(subset=["filename"]).copy()
    else:
        # if no filename column, just write the file back unchanged (but sorted if possible)
        pass

    # ensure sort keys exist
    for c in ("model", "viewtype", "sequence"):
        if c not in df.columns:
            df[c] = ""

    df = df.sort_values(by=["model", "viewtype", "sequence"], kind="stable")
    atomic_write_text(SRC, df.to_csv(index=False))
    print(f"Cleaned and wrote: {SRC}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
