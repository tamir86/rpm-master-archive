#!/usr/bin/env python3
"""
scripts/rich_to_simple.py

Converts an existing rich master CSV into a compact master_metadata_simple.csv.
Usage (from repo root):
  # make sure your venv is active and Pillow is installed
  python scripts/rich_to_simple.py --input data/master_metadata.csv --output data/master_metadata_simple.csv

The script:
 - searches the rich CSV for any field that contains 'data/photos/...'
 - if the referenced image file exists under the repo, computes width,height,filesize,sha256
 - falls back to best-effort extraction (model/view/sequence) from filename or uid
 - writes a simple CSV with the agreed schema and deterministic id values
"""
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
import re, hashlib, csv, os
from PIL import Image

def extract_model(s):
    if not s: return ""
    m = re.search(r"(BA\d{4}-\d{3})", s, flags=re.IGNORECASE)
    return m.group(1).upper() if m else ""

def extract_view_seq(fname):
    if not fname: return ("", "")
    m = re.search(r"_([a-z0-9-]+)_(\d+)\.(jpe?g|png)$", fname, re.IGNORECASE)
    if m:
        return (m.group(1).lower(), int(m.group(2)))
    m2 = re.search(r"[_-]([a-z0-9-]+)[_-](\d+)\.(jpe?g|png)$", fname, re.IGNORECASE)
    if m2:
        return (m2.group(1).lower(), int(m2.group(2)))
    return ("", "")

def sha256_of_path(p):
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for block in iter(lambda: fh.read(65536), b""):
            h.update(block)
    return h.hexdigest()

def image_info(p):
    try:
        with Image.open(p) as im:
            w,h = im.size
        fs = p.stat().st_size
        sha = sha256_of_path(p)
        return int(w), int(h), int(fs), sha
    except Exception:
        return None, None, None, ""

def find_data_photos_in_row(row):
    for col, v in row.items():
        if v and isinstance(v, str) and "data/photos" in v:
            return v.split(",")[0].strip()
    return ""

def main(args):
    repo_root = Path(args.repo).resolve()
    rich = pd.read_csv(Path(args.input), dtype=str).fillna("")
    out_rows = []

    for _, r in rich.iterrows():
        # 1) detect candidate path anywhere containing "data/photos"
        candidate = find_data_photos_in_row(r)
        if not candidate:
            # fallback common columns
            for c in ("filename","thumb_path","primary_photo","photo","style_code","uid"):
                if c in rich.columns and str(r.get(c,"")).strip():
                    candidate = str(r.get(c,"")).split(",")[0].strip()
                    break

        file_path = None
        if candidate:
            p = Path(candidate)
            if not p.is_absolute():
                p1 = repo_root / candidate
                if p1.exists():
                    file_path = p1.resolve()
                else:
                    p2 = repo_root / "data" / "photos" / candidate
                    if p2.exists():
                        file_path = p2.resolve()
            else:
                if p.exists():
                    file_path = p.resolve()

        # try to find by model if still missing
        model_guess = ""
        for col in ("style_code","uid","title"):
            if col in rich.columns and str(r.get(col,"")).strip():
                model_guess = extract_model(str(r.get(col,"")))
                if model_guess:
                    break
        if not file_path and model_guess:
            fp = repo_root / "data" / "photos" / model_guess
            if fp.exists():
                files = sorted([q for q in fp.iterdir() if q.is_file()])
                if files:
                    file_path = files[0].resolve()

        if file_path and file_path.exists():
            filename_rel = str(file_path.relative_to(repo_root)).replace(os.sep, "/")
            view, seq = extract_view_seq(file_path.name)
            if not view:
                view = (r.get("title","") or "").lower()[:20] or "unknown"
                seq = 1
            w,h,fs,sha = image_info(file_path)
            processed_at = str(r.get("processed_at","")).strip() or datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
            out_rows.append({
                "filename": filename_rel,
                "model": extract_model(filename_rel) or (r.get("style_code","") or "").upper(),
                "viewtype": view,
                "sequence": int(seq),
                "width": int(w) if w else "",
                "height": int(h) if h else "",
                "filesize": int(fs) if fs else "",
                "sha256": sha,
                "processed_at": processed_at,
                "source": r.get("acquisition_source","") or r.get("source","") or "automated",
                "uid": r.get("uid","") or "",
                "sha256_manifest_path": r.get("sha256_manifest_path","") or "",
                "notes": r.get("notes","") or ""
            })
        else:
            # missing file placeholder
            uid = r.get("uid","") or ""
            out_rows.append({
                "filename": "",
                "model": extract_model(uid),
                "viewtype": "",
                "sequence": "",
                "width": "",
                "height": "",
                "filesize": "",
                "sha256": "",
                "processed_at": r.get("processed_at","") or "",
                "source": r.get("acquisition_source","") or "automated",
                "uid": uid,
                "sha256_manifest_path": r.get("sha256_manifest_path","") or "",
                "notes": (r.get("notes","") or "") + " | missing_file"
            })

    # dedupe by sha (if present)
    by_sha = {}
    no_sha = []
    for r in out_rows:
        if r.get("sha256"):
            by_sha.setdefault(r["sha256"], []).append(r)
        else:
            no_sha.append(r)

    final = []
    for sha, group in by_sha.items():
        if len(group) == 1:
            final.append(group[0])
        else:
            group_sorted = sorted(group, key=lambda rr: (0 if rr.get("model") else 1, 0 if rr.get("processed_at") else 1))
            canon = group_sorted[0]
            dup_uids = [g.get("uid","") for g in group_sorted[1:] if g.get("uid")]
            if dup_uids:
                canon["notes"] = (canon.get("notes","") or "") + " | duplicates_of:" + ",".join(dup_uids)
            final.append(canon)
    final.extend(no_sha)

    # build id and sort
    def view_order_key(v):
        order = {"front":0,"back":1,"side":2,"detail":3,"tag":4,"strap":5,"unknown":99}
        return order.get(v,50)
    for r in final:
        shaval = r.get("sha256") or hashlib.sha256((r.get("filename","") or "").encode()).hexdigest()
        sha8 = shaval[:8]
        model = r.get("model") or "UNKNOWN"
        view = r.get("viewtype") or "unknown"
        seq = str(r.get("sequence") or "1").zfill(2)
        r["id"] = f"{model}_{view}_{seq}_{sha8}"

    final_sorted = sorted(final, key=lambda r: (r.get("model") or "ZZZZ", view_order_key(r.get("viewtype","unknown")), int(r.get("sequence") or 999), r.get("filename") or ""))

    header = ["id","filename","model","viewtype","sequence","width","height","filesize","sha256","processed_at","source","uid","sha256_manifest_path","notes"]
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path,"w",newline="",encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for r in final_sorted:
            writer.writerow({k: (r.get(k,"") if r.get(k) is not None else "") for k in header})

    print("Wrote", out_path, "rows:", len(final_sorted))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to rich master CSV (e.g. data/master_metadata.csv)")
    p.add_argument("--output", required=True, help="Path to simple CSV output (e.g. data/master_metadata_simple.csv)")
    p.add_argument("--repo", default=".", help="Repo root (default: .)")
    args = p.parse_args()
    main(args)
