#!/usr/bin/env python
"""
Repair master_metadata.csv by parsing model/viewtype/sequence from the filename
even if extra prefixes exist (e.g., 'BA2037-089_01_BA2037-089_front_01.jpeg').
"""

from __future__ import annotations
from pathlib import Path
import re
import pandas as pd
from scripts.utils.atomic_write import atomic_write_text

MASTER = Path("data/master_metadata.csv")
SCHEMA_ANYWHERE = re.compile(r"(BA\d{4}-\d{3})_([a-z]+)_(\d{2})\.[a-z0-9]+", re.I)

def parse_any(s: str):
    m = SCHEMA_ANYWHERE.search(s or "")
    if not m:
        return None
    model, view, seq = m.group(1).upper(), m.group(2).lower(), int(m.group(3))
    return model, view, f"{seq:02d}"

def main() -> int:
    if not MASTER.exists():
        print("No master file to repair.")
        return 0
    df = pd.read_csv(MASTER)

    # Ensure required columns exist
    for c in ("filename","model","viewtype","sequence"):
        if c not in df.columns:
            df[c] = ""

    repaired = 0
    for i, row in df.iterrows():
        fn = str(row.get("filename", ""))
        # try whole filename; if it contains paths or prefixes it still works
        got = parse_any(fn)
        if not got:
            continue
        model, view, seq = got

        changed = False
        if pd.isna(row.get("model")) or not str(row.get("model")):
            df.at[i, "model"] = model; changed = True
        if row.get("viewtype") in ("", "unknown") or pd.isna(row.get("viewtype")):
            df.at[i, "viewtype"] = view; changed = True
        try:
            cur_seq = str(row.get("sequence", ""))
            if not cur_seq.isdigit() or int(cur_seq) == 0:
                df.at[i, "sequence"] = seq; changed = True
        except Exception:
            df.at[i, "sequence"] = seq; changed = True

        repaired += 1 if changed else 0

    # Keep column order as-is, write atomically
    atomic_write_text(MASTER, df.to_csv(index=False))
    print(f"Repaired rows (updated >=1 field): {repaired}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
