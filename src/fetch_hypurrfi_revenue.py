#!/usr/bin/env python3
import csv, json, sys, os, time
from urllib.request import Request, urlopen
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = DATA_DIR / "hypurrfi_revenue_daily.csv"
OUT_HOURLY = DATA_DIR / "hypurrfi_revenue_hourly.csv"   # NEW
DEBUG_JSON = DATA_DIR / "hypurrfi_revenue_debug.json"

SLUG = "hypurrfi"
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
    # Prefer totalDataChart; fallback to dailyDataChart, possibly nested under data
    if isinstance(j, dict):
        for k in ("totalDataChart", "dailyDataChart"):
            arr = j.get(k)
            if isinstance(arr, list) and arr:
                return arr
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

def write_daily_csv(rows: List[Tuple[str, float]]) -> None:
    if not rows:
        OUT_CSV.write_text("date,daily_revenue_usd\n")
        sys.exit("⚠️  No revenue rows; see hypurrfi_revenue_debug.json")
    with OUT_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "daily_revenue_usd"])
        for d, v in rows:
            w.writerow([d, f"{v:.6f}"])
    log(f"✅ wrote {OUT_CSV} with {len(rows)} rows")

def append_hourly_sample(latest_date: str, latest_value: float) -> None:
    """
    Append an 'as observed' hourly sample. One row per run.
    Columns: observed_at_utc, asof_date, daily_revenue_usd
    Skip if we've already written a row for this exact minute.
    """
    observed = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    row = [observed.isoformat(), latest_date, f"{latest_value:.6f}"]

    # Avoid duplicates within the same minute
    if OUT_HOURLY.exists():
        try:
            *_, last = OUT_HOURLY.read_text().strip().splitlines()
            if last:
                last_minute = last.split(",")[0]
                if last_minute == row[0]:
                    log("Hourly: already recorded this minute; skipping")
                    return
        except Exception:
            pass

    new_file = not OUT_HOURLY.exists()
    with OUT_HOURLY.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["observed_at_utc", "asof_date", "daily_revenue_usd"])
        w.writerow(row)
    log(f"🕒 appended hourly sample to {OUT_HOURLY}")

def main():
    j = fetch_first_json(URLS)
    series = pick_series(j)
    rows = normalize_to_rows(series)
    write_daily_csv(rows)
    if rows:
        latest_date, latest_value = rows[-1]
        append_hourly_sample(latest_date, latest_value)

if __name__ == "__main__":
    main()