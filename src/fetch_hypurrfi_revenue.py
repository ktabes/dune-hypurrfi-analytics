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

OUT_DAILY = DATA_DIR / "hypurrfi_revenue.csv"
OUT_HOURLY = DATA_DIR / "hypurrfi_revenue_hourly.csv"
DEBUG_JSON = DATA_DIR / "hypurrfi_revenue_debug.json"

SLUG = "hypurrfi"
URLS = [
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyProtocolRevenue",
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyRevenue",
]
HDRS = {"User-Agent": "ktabes-hypurrfi-etl/2.1 (+github.com/ktabes)", "Accept": "application/json"}

def log(m: str): print(f"[revenue] {m}", flush=True)

def _get(url: str, timeout: int = 45) -> Any:
    req = Request(url, headers=HDRS, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        log(f"GET {url} → {resp.status}, {len(raw)} bytes")
        j = json.loads(raw.decode("utf-8"))
        DEBUG_JSON.write_text(json.dumps(j, indent=2, sort_keys=True))
        return j

def fetch_first_json(urls: List[str]) -> Any:
    last = None
    for u in urls:
        try:
            return _get(u)
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            last = e
            log(f"Error: {e}")
    raise SystemExit(f"❌ failed to fetch any revenue endpoint: {last}")

def pick_series(j: Any) -> List[Any]:
    """Prefer totalDataChart; fallback to dailyDataChart; also check nested under 'data'."""
    if isinstance(j, dict):
        for k in ("totalDataChart", "dailyDataChart"):
            arr = j.get(k)
            if isinstance(arr, list) and arr:
                return arr
        data = j.get("data", {})
        if isinstance(data, dict):
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
            ts = None; val = None
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                ts, val = item[0], item[1]
            elif isinstance(item, dict):
                ts = item.get("date") or item.get("timestamp") or item.get("time")
                val = (
                    item.get("value")
                    or item.get("revenue")
                    or item.get("protocolRevenue")
                    or item.get("dailyRevenue")
                )
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
        OUT_DAILY.write_text("date,daily_revenue_usd\n")
        sys.exit("⚠️  No revenue rows; see hypurrfi_revenue_debug.json")
    with OUT_DAILY.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "daily_revenue_usd"])
        for d, v in rows:
            w.writerow([d, f"{v:.6f}"])
    log(f"✅ wrote {OUT_DAILY} with {len(rows)} rows")

# -------- Hourly "as observed" logging (aligned to top-of-hour) --------
def read_existing_hour_slots() -> set[int]:
    """Return set of epoch seconds at top-of-hour already recorded (UTC)."""
    slots: set[int] = set()
    if OUT_HOURLY.exists():
        try:
            with OUT_HOURLY.open("r", newline="") as f:
                r = csv.DictReader(f)
                for rec in r:
                    ts_str = rec.get("timestamp_utc")
                    if not ts_str: continue
                    # ISO with Z or offset supported
                    try:
                        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except ValueError:
                        dt = datetime.fromisoformat(ts_str).astimezone(timezone.utc)
                    slots.add(int(dt.timestamp()))
        except Exception as e:
            log(f"Hourly read warning: {e}")
    return slots

def top_of_hour(now_utc: datetime) -> datetime:
    return now_utc.replace(minute=0, second=0, microsecond=0)

def append_hourly_sample(hour_slot: datetime, latest_date: str, latest_value: float) -> None:
    """
    Append exactly one row per UTC hour.
    Columns: timestamp_utc, date_utc, hour_utc, daily_revenue_usd
    """
    new_file = not OUT_HOURLY.exists()
    with OUT_HOURLY.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp_utc", "date_utc", "hour_utc", "daily_revenue_usd"])
        w.writerow([
            hour_slot.isoformat().replace("+00:00","Z"),
            latest_date,
            f"{hour_slot.hour:02d}",
            f"{latest_value:.6f}",
        ])
    log(f"🕒 appended {hour_slot.isoformat()}Z → {latest_value:.6f}")

def main():
    now = datetime.now(timezone.utc)
    hour_slot = top_of_hour(now)

    j = fetch_first_json(URLS)
    series = pick_series(j)
    rows = normalize_to_rows(series)
    write_daily_csv(rows)

    if not rows:
        return
    latest_date, latest_value = rows[-1]

    # De-dup per hour
    existing = read_existing_hour_slots()
    slot_ts = int(hour_slot.timestamp())
    if slot_ts in existing:
        log("Hourly: already recorded this hour; skipping")
        return

    append_hourly_sample(hour_slot, latest_date, latest_value)

if __name__ == "__main__":
    main()