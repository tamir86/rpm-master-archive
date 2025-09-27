#!/usr/bin/env python
"""
Ensure `sequence` is a zero-padded 2-char string and rebuild `id`
as {model}_{viewtype}_{sequence}_{sha8} when sha256/model exist.
"""
from pathlib import Path
import pandas as pd
from scripts.utils.atomic_write import atomic_write_text

MASTER = Path("data/master_metadata.csv")

def main():
    if not MASTER.exists():
        print("No master file found.")
        return 0
    df = pd.read_csv(MASTER)

    # Ensure columns exist
    for c in ("model","viewtype","sequence","sha256","id","filename"):
        if c not in df.columns:
            df[c] = ""

    # Force sequence to string and zero-pad to 2
    df["sequence"] = df["sequence"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["sequence"] = df["sequence"].str.extract(r"(\d+)", expand=False).fillna("0")
    df["sequence"] = df["sequence"].astype(int).astype(str).str.zfill(2)

    # Rebuild id where possible
    sha8 = df["sha256"].fillna("").astype(str).str[:8]
    can = (
        df["model"].astype(str).str.len().gt(0)
        & df["viewtype"].astype(str).str.len().gt(0)
        & df["sequence"].astype(str).str.len().gt(0)
    )
    df.loc[can, "id"] = (
        df.loc[can, "model"].astype(str) + "_"
        + df.loc[can, "viewtype"].astype(str) + "_"
        + df.loc[can, "sequence"].astype(str) + "_"
        + sha8[can]
    )

    # Stable sort for nice diffs
    for c in ("model","viewtype","sequence","filename"):
        if c not in df.columns:
            df[c] = ""
    df = df.sort_values(by=["model","viewtype","sequence","filename"], kind="stable")

    atomic_write_text(MASTER, df.to_csv(index=False))
    print("Normalized: zero-padded sequence and refreshed ids.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
