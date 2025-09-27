#!/usr/bin/env python
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

def load_config(path: str | Path) -> Dict[str, Any]:
    """Load YAML config; return {} if file missing or unreadable."""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        import yaml  # requires pyyaml
    except Exception:
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def pick(d: Dict[str, Any], *keys: str, default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur
