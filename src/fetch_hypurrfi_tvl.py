#!/usr/bin/env python3
import csv, json, sys, os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = DATA_DIR / "hypurrfi_tvl_hl1.csv"
DEBUG_JSON = DATA_DIR / "hypurrfi_debug.json"

SLUG = "hypurrfi"
URLS = [
    f"https://api.llama.fi/updatedProtocol/{SLUG}",
    f"https://api.llama.fi/protocol/{SLUG}",
]

def log(msg: str): print(f"[hypurrfi] {msg}")

def fetch_json() -> Any:
    last_err = None
    headers = {
        "User-Agent": "ktabes-hypurrfi-etl/1.1 (+github.com/ktabes)",
        "Accept": "application/json",
    }
    for u in URLS:
        try:
            log(f"GET {u}")
            req = Request(u, headers=headers, method="GET")
            with urlopen(req, timeout=30) as resp:
                raw = resp.read()
                log(f"Status {resp.status}, {len(raw)} bytes")
                j = json.loads(raw.decode("utf-8"))
                DEBUG_JSON.write_text(json.dumps(j, indent=2, sort_keys=True))
                log(f"Wrote debug JSON to {DEBUG_JSON}")
                return j
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            last_err = e
            log(f"Error on {u}: {e}")
    raise SystemExit(f"❌ Failed to fetch DeFiLlama JSON: {last_err}")

def pick_series(j: Any) -> List[Any]:
    if not isinstance(j, dict): return []
    chain = j.get("chainTvls") or {}
    for key in ("Hyperliquid L1", "Hyperliquid"):
        blk = chain.get(key)
        if isinstance(blk, dict) and isinstance(blk.get("tvl"), list):
            log(f"Using chainTvls['{key}'].tvl with {len(blk['tvl'])} points")
            return blk["tvl"]
    if isinstance(j.get("tvl"), list):
        log(f"Using top-level 'tvl' with {len(j['tvl'])} points")
        return j["tvl"]
    return []

def unix_to_date(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()

def normalize_series(series: List[Any]) -> List[Tuple[str, float]]:
    rows: List[Tuple[str, float]] = []
    bad = 0
    for item in series:
        try:
            ts = None; val = None
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                ts, val = item[0], item[1]
            elif isinstance(item, dict):
                ts = item.get("date") or item.get("timestamp") or item.get("time")
                for k in ("totalLiquidityUSD","totalLiquidityUsd","totalLiquidity","tvl","value","liquidityUSD"):
                    if k in item:
                        val = item[k]; break
            if ts is None or val is None:
                bad += 1; continue
            dt = unix_to_date(int(ts)); tvl = float(val)
            rows.append((dt, tvl))
        except Exception:
            bad += 1
    log(f"Parsed {len(rows)} rows; skipped {bad} malformed")
    return rows

def load_existing() -> Dict[str, float]:
    existing: Dict[str, float] = {}
    if OUTFILE.exists():
        with OUTFILE.open("r", newline="") as f:
            r = csv.DictReader(f)
            for rec in r:
                try: existing[rec["date"]] = float(rec["tvl_usd"])
                except Exception: pass
        log(f"Loaded {len(existing)} existing rows from {OUTFILE}")
    return existing

def write_csv(data: Dict[str, float]) -> None:
    with OUTFILE.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","tvl_usd"])
        for dt in sorted(data.keys()):
            w.writerow([dt, f"{data[dt]:.6f}"])
    log(f"✅ Wrote {OUTFILE} with {len(data)} rows")

def main():
    j = fetch_json()
    rows = normalize_series(pick_series(j))
    existing = load_existing()
    for dt, tvl in rows: existing[dt] = tvl
    if not existing:
        OUTFILE.write_text("date,tvl_usd\n")
        sys.exit("⚠️  No data rows found. See data/hypurrfi_debug.json.")
    write_csv(existing)

if __name__ == "__main__":
    main()