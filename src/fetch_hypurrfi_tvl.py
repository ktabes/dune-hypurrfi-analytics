#!/usr/bin/env python3
import csv, json, sys, os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(os.getenv("GITHUB_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
DATA_DIR = (REPO_ROOT / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTFILE_DAILY = DATA_DIR / "hypurrfi_tvl.csv"
OUTFILE_HOURLY = DATA_DIR / "hypurrfi_tvl_hourly.csv"
DEBUG_JSON = DATA_DIR / "hypurrfi_debug.json"

SLUG = "hypurrfi"
URLS = [
    f"https://api.llama.fi/updatedProtocol/{SLUG}",
    f"https://api.llama.fi/protocol/{SLUG}",
]
UA = "ktabes-hypurrfi-etl/2.1 (+github.com/ktabes)"
CHAIN_KEYS = ("Hyperliquid L1", "Hyperliquid")  # prefer HL1 names


def log(msg: str): print(f"[tvl] {msg}", flush=True)


def _get(url: str, timeout: int = 30) -> Any:
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        log(f"GET {url} → {resp.status}, {len(raw)} bytes")
        j = json.loads(raw.decode("utf-8"))
        DEBUG_JSON.write_text(json.dumps(j, indent=2, sort_keys=True))
        return j


def fetch_json() -> Any:
    last_err = None
    for u in URLS:
        try:
            return _get(u)
        except (HTTPError, URLError, TimeoutError, ValueError) as e:
            last_err = e
            log(f"Error on {u}: {e}")
    raise SystemExit(f"❌ Failed to fetch DeFiLlama JSON: {last_err}")


def pick_series(j: Any) -> List[Any]:
    """Prefer chainTvls['Hyperliquid L1'|'Hyperliquid'].tvl else top-level tvl (list of [ts,val] or objects)."""
    if not isinstance(j, dict): return []
    chain = j.get("chainTvls") or {}
    for key in CHAIN_KEYS:
        blk = chain.get(key)
        if isinstance(blk, dict) and isinstance(blk.get("tvl"), list):
            log(f"Using chainTvls['{key}'].tvl ({len(blk['tvl'])} pts)")
            return blk["tvl"]
    if isinstance(j.get("tvl"), list):
        log(f"Using top-level tvl ({len(j['tvl'])} pts)")
        return j["tvl"]
    return []


def _extract_ts_val(item: Any) -> Tuple[int, float] | None:
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        try:
            return int(item[0]), float(item[1])
        except Exception:
            return None
    if isinstance(item, dict):
        ts = item.get("date") or item.get("timestamp") or item.get("time")
        for k in ("totalLiquidityUSD","totalLiquidityUsd","totalLiquidity","tvl","value","liquidityUSD"):
            if k in item:
                try:
                    return int(ts), float(item[k])
                except Exception:
                    return None
    return None


def normalize_series(series: List[Any]) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    bad = 0
    for it in series:
        pair = _extract_ts_val(it)
        if pair:
            out.append(pair)
        else:
            bad += 1
    out.sort(key=lambda x: x[0])
    log(f"Parsed {len(out)} rows; skipped {bad} malformed")
    return out


def write_daily_csv(series: List[Tuple[int, float]]) -> None:
    """Write daily file: date,tvl_usd (ISO date, 6 decimals)"""
    rows: Dict[str, float] = {}
    for ts, val in series:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        rows[dt] = float(val)
    if not rows:
        OUTFILE_DAILY.write_text("date,tvl_usd\n")
        log("⚠️  No daily data rows found; wrote header only")
        return
    with OUTFILE_DAILY.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","tvl_usd"])
        for dt in sorted(rows.keys()):
            w.writerow([dt, f"{rows[dt]:.6f}"])
    log(f"✅ Wrote {OUTFILE_DAILY} with {len(rows)} rows")


def read_existing_hours() -> set[int]:
    """Return set of epoch seconds for hour slots already recorded (UTC)."""
    slots: set[int] = set()
    if OUTFILE_HOURLY.exists():
        try:
            with OUTFILE_HOURLY.open("r", newline="") as f:
                r = csv.DictReader(f)
                for rec in r:
                    ts = rec.get("timestamp_utc")
                    if ts:
                        try:
                            dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
                        except ValueError:
                            dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
                        slots.add(int(dt.timestamp()))
        except Exception as e:
            log(f"Hourly read warning: {e}")
    return slots


def get_current_hour_slot(now_utc: datetime) -> datetime:
    return now_utc.replace(minute=0, second=0, microsecond=0)


def latest_series_value(series: List[Tuple[int, float]]) -> Tuple[int, float]:
    if not series:
        raise SystemExit("No TVL series available")
    return series[-1]


def append_hourly(observed_slot: datetime, tvl_usd: float) -> None:
    """Append a row for the given hour slot (UTC), de-duped."""
    new_file = not OUTFILE_HOURLY.exists()
    with OUTFILE_HOURLY.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp_utc","date_utc","hour_utc","tvl_usd"])
        date_utc = observed_slot.date().isoformat()
        hour_utc = f"{observed_slot.hour:02d}"
        w.writerow([observed_slot.isoformat().replace("+00:00","Z"), date_utc, hour_utc, f"{tvl_usd:.6f}"])
    log(f"🕒 appended {observed_slot.isoformat()}Z → {tvl_usd:.6f}")


def main():
    now = datetime.now(timezone.utc)
    slot = get_current_hour_slot(now)

    j = fetch_json()
    series = normalize_series(pick_series(j))

    write_daily_csv(series)

    _, latest_val = latest_series_value(series)
    existing = read_existing_hours()
    slot_ts = int(slot.timestamp())

    if slot_ts in existing:
        log("Hourly: already recorded this hour; skipping")
        return

    append_hourly(slot, latest_val)


if __name__ == "__main__":
    main()