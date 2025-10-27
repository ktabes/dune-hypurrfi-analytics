#!/usr/bin/env python3
import csv, json, sys, os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import io

# ----------------- Repo paths & outputs -----------------
REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_DAILY  = DATA_DIR / "hypurrfi_revenue.csv"
OUT_HOURLY = DATA_DIR / "hypurrfi_revenue_hourly.csv"
DEBUG_JSON = DATA_DIR / "hypurrfi_revenue_debug.json"

# ----------------- Source endpoints -----------------
SLUG = "hypurrfi"
# Prefer dailyProtocolRevenue, fallback to dailyRevenue
URLS = [
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyProtocolRevenue",
    f"https://api.llama.fi/summary/fees/{SLUG}?dataType=dailyRevenue",
]
HDRS = {"User-Agent": "ktabes-hypurrfi-etl/2.1 (+github.com/ktabes)", "Accept": "application/json"}

# ----------------- Optional Dune upload -----------------
DUNE_API_KEY       = os.getenv("DUNE_API_KEY")
DUNE_UPLOAD_URL    = "https://api.dune.com/api/v1/table/upload/csv"
DUNE_TABLE_DAILY   = os.getenv("DUNE_TABLE_DAILY",  "hypurrfi_daily_revenue_hl1")
DUNE_TABLE_HOURLY  = os.getenv("DUNE_TABLE_HOURLY", "hypurrfi_daily_revenue_hl1_hourly")
DUNE_TABLE_PRIVATE = os.getenv("DUNE_TABLE_PRIVATE", "false").lower() == "true"

def log(m: str): 
    print(f"[revenue] {m}", flush=True)

# ----------------- HTTP helpers -----------------
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

# ----------------- Parsing & normalization -----------------
def _prefer_hl1_from_breakdown(j: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    If totalDataChartBreakdown exists, try to pull exactly 'Hyperliquid L1'.
    Each element is [timestamp, { chain: number | {product: number, ...}, ... }]
    """
    rows: List[Tuple[str, float]] = []
    tdcbd = j.get("totalDataChartBreakdown")
    if not isinstance(tdcbd, list):
        # Sometimes nested under 'data'
        data = j.get("data", {})
        if isinstance(data, dict):
            tdcbd = data.get("totalDataChartBreakdown")

    if isinstance(tdcbd, list):
        for item in tdcbd:
            if not (isinstance(item, list) and len(item) >= 2):
                continue
            ts, payload = item[0], item[1]
            if not isinstance(payload, dict):
                continue
            hl1 = payload.get("Hyperliquid L1")
            if hl1 is None:
                continue
            # Number or per-product map
            if isinstance(hl1, dict):
                total = 0.0
                for v in hl1.values():
                    try: total += float(v or 0.0)
                    except: pass
            else:
                total = float(hl1 or 0.0)
            day = ts_to_date_str(int(ts))
            rows.append((day, total))
    return rows

def pick_series(j: Any) -> List[Any]:
    """Legacy series picker (kept for fallback): prefer totalDataChart; then dailyDataChart; also check nested under 'data'."""
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

# ----------------- Write CSVs -----------------
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

# ----------------- Dune upload (overwrite with full CSV content) -----------------
def upload_csv_to_dune(path: Path, table_name: str, description: str) -> None:
    if not DUNE_API_KEY:
        log("Dune upload skipped (no DUNE_API_KEY).")
        return
    data = path.read_text(encoding="utf-8")
    payload = {
        "data": data,
        "description": description,
        "table_name": table_name,
        "is_private": DUNE_TABLE_PRIVATE,
    }
    req = Request(DUNE_UPLOAD_URL, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-DUNE-API-KEY", DUNE_API_KEY)
    body = json.dumps(payload).encode("utf-8")
    try:
        with urlopen(req, body, timeout=90) as resp:
            raw = resp.read()
            log(f"📤 Dune upload {table_name}: {resp.status}, {len(raw)} bytes")
    except Exception as e:
        log(f"❌ Dune upload error for {table_name}: {e}")
        raise

# ----------------- Main -----------------
def main():
    now = datetime.now(timezone.utc)
    hour_slot = top_of_hour(now)

    j = fetch_first_json(URLS)

    # 1) Prefer HL1-only if breakdown exists; otherwise fallback to legacy arrays
    hl1_rows = _prefer_hl1_from_breakdown(j)
    if hl1_rows:
        rows = sorted({d: v for d, v in hl1_rows}.items())
        log(f"Using HL1 breakdown series with {len(rows)} rows")
    else:
        series = pick_series(j)
        rows = normalize_to_rows(series)

    # Write daily CSV (full history; includes today's partial)
    write_daily_csv(rows)

    # Hourly observation row (one per UTC hour)
    if rows:
        latest_date, latest_value = rows[-1]
        existing = read_existing_hour_slots()
        slot_ts = int(hour_slot.timestamp())
        if slot_ts not in existing:
            append_hourly_sample(hour_slot, latest_date, latest_value)
        else:
            log("Hourly: already recorded this hour; skipping")

    # Optional: upload both CSVs to Dune (overwrite with full history)
    try:
        upload_csv_to_dune(OUT_DAILY,  DUNE_TABLE_DAILY,  "HypurrFi daily revenue (USD) — HL1 breakdown preferred")
        if OUT_HOURLY.exists():
            upload_csv_to_dune(OUT_HOURLY, DUNE_TABLE_HOURLY, "HypurrFi daily revenue observed each UTC hour (USD)")
    except Exception:
        # Don't fail the workflow just because Dune upload had a transient error
        log("Continuing after Dune upload error")

if __name__ == "__main__":
    main()