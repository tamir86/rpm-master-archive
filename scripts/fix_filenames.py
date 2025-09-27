#!/usr/bin/env python
"""
Normalize image filenames to:
    BA####-###_<viewtype>_<NN>.<ext>

- Detects model code anywhere in the path.
- Guesses viewtype from hints (now includes 'tag').
- Chooses the next sequence (NN) by scanning the folder, so dupes become _02, _03, ...
- Dry-run by default if you pass --dry-run; writes a TSV log either way.
"""

import re
import sys
import csv
from pathlib import Path
import argparse
from collections import defaultdict

# Allowed extensions (lowercased, with dot)
EXTS = {".jpg", ".jpeg", ".png"}

# Canonical schema (used only to skip files that are already correct)
SCHEMA = re.compile(r"^(BA\d{4}-\d{3})_([a-z]+)_(\d{2})\.(jpg|jpeg|png)$", re.I)

# Hints to guess viewtype from the original filename
VIEW_HINTS = {
    "front":  ["front", "fr", "main"],
    "back":   ["back", "bk"],
    "side":   ["side", "sd", "left", "right"],
    "top":    ["top", "tp"],
    "bottom": ["bottom", "btm", "base"],
    "detail": ["detail", "close", "macro"],
    "tag":    ["tag", "label", "care", "wash", "size"],  # NEW
}

def guess_viewtype(name: str) -> str:
    low = name.lower()
    for vt, keys in VIEW_HINTS.items():
        if any(k in low for k in keys):
            return vt
    return "unknown"

def existing_max_seq(folder: Path, model: str, viewtype: str) -> int:
    """
    Look for files already named like BA####-###_<viewtype>_##.* in this folder
    and return the highest existing sequence number (int). If none, return 0.
    """
    max_seq = 0
    pat = re.compile(rf"^{re.escape(model)}_{re.escape(viewtype)}_(\d{{2}})\.", re.I)
    for ext in EXTS:
        for f in folder.glob(f"{model}_{viewtype}_*{ext}"):
            m = pat.match(f.name)
            if m:
                try:
                    max_seq = max(max_seq, int(m.group(1)))
                except ValueError:
                    pass
    return max_seq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Folder to scan for images")
    ap.add_argument("--dry-run", action="store_true", help="Only log changes (no writes)")
    ap.add_argument("--log", default="logs/fix_filenames.tsv", help="TSV log path")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: root not found: {root}", file=sys.stderr)
        return 2

    seq_cache: dict[tuple[str, str], int] = defaultdict(int)
    changes: list[tuple[str, str, str]] = []

    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in EXTS:
            continue

        name = p.name
        if SCHEMA.match(name):
            # already compliant
            continue

        # find model code anywhere in the path
        m = re.search(r"(BA\d{4}-\d{3})", p.as_posix(), re.I)
        if not m:
            changes.append((str(p), "", "SKIP:no-model"))
            continue

        model = m.group(1).upper()
        viewtype = guess_viewtype(p.stem)

        # Choose proper sequence based on what's already in THIS folder
        photo_dir = p.parent
        start_seq = existing_max_seq(photo_dir, model, viewtype) + 1
        # keep a cache so multiple renames in the same run stack correctly
        key = (model, viewtype)
        seq_cache[key] = max(seq_cache[key], start_seq)

        ext = p.suffix.lower().lstrip(".")
        seqnum = f"{seq_cache[key]:02d}"
        target = p.with_name(f"{model}_{viewtype}_{seqnum}.{ext}")

        # if a rare collision still happens, keep incrementing the sequence cleanly
        while target.exists():
            seq_cache[key] += 1
            seqnum = f"{seq_cache[key]:02d}"
            target = p.with_name(f"{model}_{viewtype}_{seqnum}.{ext}")

        # Decide whether this is a real rename (skip no-ops)
        if str(p) != str(target):
            changes.append((str(p), str(target), "RENAME"))

            # Apply if not dry-run
            if not args.dry_run:
                p.rename(target)

    # Write log
    Path(args.log).parent.mkdir(parents=True, exist_ok=True)
    with open(args.log, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["src", "dst", "action"])
        w.writerows(changes)

    planned = sum(1 for *_rest, a in changes if a.startswith("RENAME"))
    print(f"planned_renames={planned}")
    print(f"log={args.log}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
