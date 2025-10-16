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

OUT_CSV = DATA_DIR / "hypurrfi_revenue_daily.csv"
DEBUG_JSON = DATA_DIR / "hypurrfi_revenue_debug.json"

SLUG = "hypurrfi"

# We try multiple shapes/endpoints because Llama varies by protocol:
URLS = [
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyProtocolRevenue",
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyRevenue",
]

HDRS = {"User-Agent": "ktabes-hypurrfi-etl/1.1 (+github.com/ktabes)", "Accept": "application/json"}

def log(m: str): print(f"[revenue] {m}")

def fetch_first_json(urls: List[str]) -> Any:
    last = None
    for u in urls:
        try:
            log(f"GET {u}")
            req = Request(u, headers=HDRS)
            with urlopen(req, timeout=30) as resp:
                raw = resp.read()
                log(f"Status {resp.status}, {len(raw)} bytes")
                j = json.loads(raw.decode("utf-8"))
                DEBUG_JSON.write_text(json.dumps(j, indent=2, sort_keys=True))
                log(f"Wrote debug JSON → {DEBUG_JSON}")
                return j
        except Exception as e:
            last = e
            log(f"Error: {e}")
    raise SystemExit(f"❌ failed to fetch any revenue endpoint: {last}")

def pick_series(j: Any) -> List[Any]:
    """
    DeFiLlama 'summary/fees/{protocol}' usually returns:
      - totalDataChart: [[ts, value], ...]
      - dailyDataChart: [[ts, value], ...]  (sometimes used)
    We’ll check both; prefer 'totalDataChart' if it looks daily, else 'dailyDataChart'.
    Values should already be protocol revenue (USD) for the chosen dataType.
    """
    if not isinstance(j, dict): return []
    # Most common:
    for k in ("totalDataChart", "dailyDataChart"):
        arr = j.get(k)
        if isinstance(arr, list) and arr:
            return arr
    # Fallback: sometimes nested under 'data' key
    data = j.get("data", {})
    for k in ("totalDataChart", "dailyDataChart"):
        arr = data.get(k)
        if isinstance(arr, list) and arr:
            return arr
    return []

def ts_to_date_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()

def normalize_to_rows(arr: List[Any]) -> List[Tuple[str, float]]:
    rows, bad = [], 0
    for item in arr:
        try:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                ts, val = item[0], item[1]
            elif isinstance(item, dict):
                ts = item.get("date") or item.get("timestamp") or item.get("time")
                val = item.get("value") or item.get("revenue") or item.get("protocolRevenue")
            else:
                bad += 1; continue
            if ts is None or val is None:
                bad += 1; continue
            rows.append((ts_to_date_str(int(ts)), float(val)))
        except Exception:
            bad += 1
    log(f"Parsed {len(rows)} rows; skipped {bad}")
    # Deduplicate by date (keep last)
    dd: Dict[str, float] = {}
    for d, v in rows: dd[d] = v
    return sorted(dd.items(), key=lambda x: x[0])

def write_csv(rows: List[Tuple[str, float]]) -> None:
    if not rows:
        OUT_CSV.write_text("date,daily_revenue_usd\n")
        sys.exit("⚠️  No revenue rows; see hypurrfi_revenue_debug.json")
    with OUT_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "daily_revenue_usd"])
        for d, v in rows:
            w.writerow([d, f"{v:.6f}"])
    log(f"✅ wrote {OUT_CSV} with {len(rows)} rows")

def main():
    j = fetch_first_json(URLS)
    series = pick_series(j)
    rows = normalize_to_rows(series)
    write_csv(rows)

if __name__ == "__main__":
    main()
