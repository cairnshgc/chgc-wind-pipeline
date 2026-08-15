#!/usr/bin/env python3
"""
CHGC wind pipeline: eagle.io  ->  BigQuery chgc_wind.readings

Fetches the Rex Lookout weather station's wind parameters from the eagle.io API
and appends them to the club's historical archive in BigQuery.

Safe by design:
  * The API key is READ-ONLY and is never written to disk or logged.
  * Rows are MERGEd on timestamp, so re-running NEVER creates duplicates.
  * --dry-run fetches and reports but writes nothing.

Usage
-----
  # what the scheduler runs: last 36 hours, self-healing overlap
  python3 pull_eagle.py

  # see what it would do, without touching BigQuery
  python3 pull_eagle.py --dry-run

  # backfill an explicit window (UTC)
  python3 pull_eagle.py --start 2026-08-09T12:40:00Z --end 2026-08-15T12:00:00Z

The key comes from the EAGLE_API_KEY environment variable. On Cloud Run this is
injected from Secret Manager; locally you can `export EAGLE_API_KEY='...'`.
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

PROJECT = os.environ.get("BQ_PROJECT", "chgc-wind-data")
DATASET = os.environ.get("BQ_DATASET", "chgc_wind")
TABLE = os.environ.get("BQ_TABLE", "readings")
STAGE_TABLE = os.environ.get("BQ_STAGE_TABLE", "stage_eagle")
SOURCE_TAG = "eagle_api"

EAGLE_BASE = "https://api.eagle.io/api/v1"

# eagle.io node IDs for the Rex station (discovered by the 2026-08-09 spike).
# These are identifiers, NOT secrets. The API key is the secret.
# Maps: BigQuery column -> eagle.io parameter node id
PARAMS = {
    "wind_speed_kn":     "6a7138b2559bf0a76ca47b6d",  # Wind Speed   (knots)
    "wind_speed_min_kn": "6a7138b2559bf0a76ca47b6c",  # WSpd Min
    "wind_speed_max_kn": "6a7138b2559bf0a76ca47b6e",  # WSpd Max
    "wind_dir_deg":      "6a7138b2559bf0a76ca47b6a",  # Wind Direction (degrees)
    "wind_dir_min_deg":  "6a7138b2559bf0a76ca47b69",  # WDir Min
    "wind_dir_max_deg":  "6a7138b2559bf0a76ca47b6b",  # WDir Max
    "volts":             "6a7138b2559bf0a76ca47b6f",  # Battery volts
    "rssi":              "6a7138b2559bf0a76ca47b68",  # Cellular signal
}

# The legacy VDV logger wrote 99.9 kt as an error sentinel. Assume eagle.io may
# pass the same through. Anything at or above this on a wind speed is junk.
SPEED_SENTINEL = 99.9

# Sanity bounds. Readings outside these are dropped and counted, not stored.
MAX_PLAUSIBLE_KN = 120.0
PAGE_LIMIT = 1000          # rows per eagle.io request
MAX_PAGES = 500            # hard stop, guards against a paging bug looping forever
DEFAULT_WINDOW_HOURS = 36  # overlap so a missed run is covered by the next one

# Freshness guard. After a successful run the newest reading in the archive should
# be recent. If it is not, SOMETHING is wrong even though this job "worked":
# the station may be dead (as it was Oct 2025 - Feb 2026), eagle.io may have
# stopped serving us, or the sensor may have failed. We log an ERROR so the
# Cloud Monitoring alert fires. 48h tolerates one missed nightly run.
STALE_HOURS = 48

log = logging.getLogger("pull_eagle")


class CloudLoggingFormatter(logging.Formatter):
    """
    Emit each log line as a single JSON object.

    Cloud Run reads plain text as unstructured, with NO severity, so an ERROR
    from Python arrives in Cloud Logging as an ordinary line that happens to
    contain the word "ERROR". Alerts cannot see it, and the alert email ends up
    with an empty body. Emitting JSON with a "severity" key makes Cloud Logging
    treat it as a real error, so the alert email carries the actual message and
    you can triage without opening a terminal.

    Falls back to plain text when not running on Cloud Run, so local runs stay
    readable.
    """

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        })


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    on_cloud_run = bool(os.environ.get("CLOUD_RUN_JOB"))
    if on_cloud_run:
        handler.setFormatter(CloudLoggingFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


# --------------------------------------------------------------------------
# eagle.io
# --------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(raw) -> datetime:
    """eagle.io timestamps are UTC ISO-8601, sometimes with milliseconds."""
    if isinstance(raw, (int, float)):
        # Defensive: some JTS payloads use epoch milliseconds.
        return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
    s = str(raw).replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def eagle_get(path: str, key: str, params: dict, retries: int = 4):
    """GET with exponential backoff. Returns parsed JSON, or None on failure."""
    url = EAGLE_BASE + path + "?" + urllib.parse.urlencode(params)
    headers = {"X-Api-Key": key, "Accept": "application/json"}

    for attempt in range(retries):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            # 4xx other than rate-limiting will not fix themselves: fail fast.
            if e.code in (401, 403):
                raise SystemExit(
                    f"eagle.io rejected the API key (HTTP {e.code}). "
                    f"Check the secret value. Response: {body}"
                )
            if 400 <= e.code < 500 and e.code != 429:
                log.error("HTTP %s on %s: %s", e.code, path, body)
                return None
            log.warning("HTTP %s on %s (attempt %s): %s",
                        e.code, path, attempt + 1, body)
        except Exception as e:  # network blip, timeout
            log.warning("Request error on %s (attempt %s): %s",
                        path, attempt + 1, e)

        if attempt < retries - 1:
            time.sleep(2 ** attempt)

    log.error("Giving up on %s after %s attempts", path, retries)
    return None


def fetch_param(key: str, node_id: str, start: datetime, end: datetime) -> dict:
    """
    Fetch one parameter's history for the window.
    Returns {datetime -> float}. Pages until the window is exhausted.
    """
    out = {}
    cursor = start

    for page in range(MAX_PAGES):
        payload = eagle_get("/historic", key, {
            "params": node_id,
            "startTime": _iso(cursor),
            "endTime": _iso(end),
            "limit": PAGE_LIMIT,
        })
        if payload is None:
            break

        rows = payload.get("data") or []
        if not rows:
            break

        newest = cursor
        for row in rows:
            try:
                ts = _parse_ts(row.get("ts"))
            except Exception:
                continue
            # JTS shape: {"ts": ..., "f": {"0": {"v": <value>}}}
            field = (row.get("f") or {}).get("0") or {}
            value = field.get("v")
            if value is None:
                continue
            try:
                out[ts] = float(value)
            except (TypeError, ValueError):
                continue
            if ts > newest:
                newest = ts

        if len(rows) < PAGE_LIMIT:
            break
        if newest <= cursor:
            # No forward progress: stop rather than loop forever.
            log.warning("Paging stalled for %s at %s", node_id, _iso(cursor))
            break
        cursor = newest + timedelta(milliseconds=1)

    return out


def fetch_window(key: str, start: datetime, end: datetime):
    """Fetch every parameter and pivot into wide rows keyed by timestamp."""
    by_ts = {}
    for column, node_id in PARAMS.items():
        series = fetch_param(key, node_id, start, end)
        log.info("  %-18s %6d readings", column, len(series))
        for ts, value in series.items():
            by_ts.setdefault(ts, {})[column] = value
    return by_ts


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------

def clean(by_ts: dict):
    """
    Apply the same rules the historical VDV transform used, so both sources in
    the table mean the same thing. Returns (rows, stats).
    """
    rows, dropped_sentinel, dropped_range, dropped_empty = [], 0, 0, 0

    for ts in sorted(by_ts):
        rec = dict(by_ts[ts])

        bad = False
        for col in ("wind_speed_kn", "wind_speed_min_kn", "wind_speed_max_kn"):
            v = rec.get(col)
            if v is None:
                continue
            if v >= SPEED_SENTINEL:
                rec[col] = None
                if col == "wind_speed_kn":
                    dropped_sentinel += 1
                    bad = True
            elif v < 0 or v > MAX_PLAUSIBLE_KN:
                rec[col] = None
                if col == "wind_speed_kn":
                    dropped_range += 1
                    bad = True

        for col in ("wind_dir_deg", "wind_dir_min_deg", "wind_dir_max_deg"):
            v = rec.get(col)
            if v is not None and not (0 <= v <= 360):
                rec[col] = None

        # A row with no usable wind reading carries no value for us.
        if bad or (rec.get("wind_speed_kn") is None
                   and rec.get("wind_dir_deg") is None):
            if not bad:
                dropped_empty += 1
            continue

        rows.append({
            "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "wind_speed_kn": rec.get("wind_speed_kn"),
            "wind_speed_min_kn": rec.get("wind_speed_min_kn"),
            "wind_speed_max_kn": rec.get("wind_speed_max_kn"),
            "wind_dir_deg": rec.get("wind_dir_deg"),
            "wind_dir_min_deg": rec.get("wind_dir_min_deg"),
            "wind_dir_max_deg": rec.get("wind_dir_max_deg"),
            "volts": rec.get("volts"),
            "rssi": rec.get("rssi"),
            "source": SOURCE_TAG,
        })

    return rows, {
        "sentinel": dropped_sentinel,
        "out_of_range": dropped_range,
        "empty": dropped_empty,
    }


# --------------------------------------------------------------------------
# BigQuery
# --------------------------------------------------------------------------

def check_freshness() -> bool:
    """
    Ask the archive how old its newest reading is.

    This catches the failure mode that the job itself CANNOT detect: the job
    runs perfectly, eagle.io answers politely, and there is simply no data
    because the station is dead. Exactly what happened Oct 2025 - Feb 2026.
    From the job's point of view that is a success, which is why we check the
    outcome rather than the machinery.

    Returns True if fresh. Logs an ERROR (which triggers the alert) if not.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    row = list(client.query(
        f"SELECT MAX(ts) AS newest FROM `{PROJECT}.{DATASET}.{TABLE}`"
    ).result())[0]

    if row.newest is None:
        log.error("STALE ARCHIVE: the readings table is empty.")
        return False

    newest = row.newest.replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600

    if age_hours > STALE_HOURS:
        log.error(
            "STALE ARCHIVE: newest reading is %s, which is %.1f hours old "
            "(threshold %d). The job ran fine, so the likely cause is the Rex "
            "station being offline or eagle.io no longer serving data. "
            "Check the public dashboard and contact CBased if needed.",
            _iso(newest), age_hours, STALE_HOURS,
        )
        return False

    log.info("Freshness OK: newest reading %s (%.1f hours old).",
             _iso(newest), age_hours)
    return True


def load_and_merge(rows: list) -> dict:
    """
    Load rows into a staging table, then MERGE on ts into the archive.
    MERGE means a timestamp that already exists (from the VDV history or from a
    previous run of this job) is left alone. Re-running is always safe.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    stage_ref = f"{PROJECT}.{DATASET}.{STAGE_TABLE}"
    table_ref = f"{PROJECT}.{DATASET}.{TABLE}"

    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("wind_speed_kn", "FLOAT64"),
        bigquery.SchemaField("wind_speed_min_kn", "FLOAT64"),
        bigquery.SchemaField("wind_speed_max_kn", "FLOAT64"),
        bigquery.SchemaField("wind_dir_deg", "FLOAT64"),
        bigquery.SchemaField("wind_dir_min_deg", "FLOAT64"),
        bigquery.SchemaField("wind_dir_max_deg", "FLOAT64"),
        bigquery.SchemaField("volts", "FLOAT64"),
        bigquery.SchemaField("rssi", "FLOAT64"),
        bigquery.SchemaField("source", "STRING"),
    ]

    log.info("Loading %d rows into staging %s", len(rows), stage_ref)
    job = client.load_table_from_json(
        rows, stage_ref,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()

    before = list(client.query(
        f"SELECT COUNT(*) AS n FROM `{table_ref}`"
    ).result())[0].n

    merge_sql = f"""
    MERGE `{table_ref}` AS target
    USING (
      SELECT ts,
             ANY_VALUE(wind_speed_kn)     AS wind_speed_kn,
             ANY_VALUE(wind_speed_min_kn) AS wind_speed_min_kn,
             ANY_VALUE(wind_speed_max_kn) AS wind_speed_max_kn,
             ANY_VALUE(wind_dir_deg)      AS wind_dir_deg,
             ANY_VALUE(wind_dir_min_deg)  AS wind_dir_min_deg,
             ANY_VALUE(wind_dir_max_deg)  AS wind_dir_max_deg,
             ANY_VALUE(volts)             AS volts,
             ANY_VALUE(rssi)              AS rssi,
             ANY_VALUE(source)            AS source
      FROM `{stage_ref}`
      GROUP BY ts
    ) AS staged
    ON target.ts = staged.ts
    WHEN NOT MATCHED THEN INSERT (
      ts, wind_speed_kn, wind_speed_min_kn, wind_speed_max_kn,
      wind_dir_deg, wind_dir_min_deg, wind_dir_max_deg, volts, rssi, source
    ) VALUES (
      staged.ts, staged.wind_speed_kn, staged.wind_speed_min_kn,
      staged.wind_speed_max_kn, staged.wind_dir_deg, staged.wind_dir_min_deg,
      staged.wind_dir_max_deg, staged.volts, staged.rssi, staged.source
    )
    """
    merge_job = client.query(merge_sql)
    merge_job.result()

    after = list(client.query(
        f"SELECT COUNT(*) AS n FROM `{table_ref}`"
    ).result())[0].n

    client.delete_table(stage_ref, not_found_ok=True)

    return {
        "before": before,
        "after": after,
        "inserted": after - before,
        "skipped_existing": len(rows) - (after - before),
    }


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="window start, UTC ISO-8601 (e.g. 2026-08-09T12:40:00Z)")
    ap.add_argument("--end", help="window end, UTC ISO-8601. Defaults to now.")
    ap.add_argument("--hours", type=int, default=DEFAULT_WINDOW_HOURS,
                    help=f"look back this many hours when --start is omitted "
                         f"(default {DEFAULT_WINDOW_HOURS})")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, but write nothing to BigQuery")
    args = ap.parse_args()

    setup_logging()

    key = os.environ.get("EAGLE_API_KEY")
    if not key:
        log.error("EAGLE_API_KEY is not set. On Cloud Run it comes from Secret "
                  "Manager; locally use: export EAGLE_API_KEY='...'")
        return 2

    end = _parse_ts(args.end) if args.end else datetime.now(timezone.utc)
    start = _parse_ts(args.start) if args.start else end - timedelta(hours=args.hours)

    if start >= end:
        log.error("Start (%s) is not before end (%s).", _iso(start), _iso(end))
        return 2

    span_days = (end - start).total_seconds() / 86400
    log.info("Window: %s  ->  %s  (%.1f days)", _iso(start), _iso(end), span_days)
    if args.dry_run:
        log.info("DRY RUN: nothing will be written to BigQuery.")

    log.info("Fetching from eagle.io...")
    by_ts = fetch_window(key, start, end)
    if not by_ts:
        # Deliberately ERROR, not WARNING. On the daily schedule this means a
        # day and a half produced nothing, which is never normal for a healthy
        # station. Raising it here is what makes the alert fire.
        if args.dry_run:
            log.warning("No data returned for this window (dry run).")
            return 0
        log.error(
            "NO DATA returned by eagle.io for %s -> %s. Likely causes: the Rex "
            "station is offline, or eagle.io no longer holds history this far "
            "back. Check the public dashboard first.", _iso(start), _iso(end),
        )
        return 1

    rows, stats = clean(by_ts)
    log.info("Fetched %d timestamps; %d rows survive cleaning "
             "(dropped: %d sentinel, %d out-of-range, %d empty)",
             len(by_ts), len(rows), stats["sentinel"],
             stats["out_of_range"], stats["empty"])

    if rows:
        log.info("Range in payload: %s  ->  %s", rows[0]["ts"], rows[-1]["ts"])

    if not rows:
        log.warning("Nothing usable to load.")
        return 0

    if args.dry_run:
        log.info("DRY RUN complete. Would have merged %d rows.", len(rows))
        for r in rows[:3]:
            log.info("  sample: %s", r)
        return 0

    result = load_and_merge(rows)
    log.info("MERGE done. Table %d -> %d rows (+%d new, %d already present).",
             result["before"], result["after"],
             result["inserted"], result["skipped_existing"])

    # The machinery worked. Now check the outcome.
    fresh = check_freshness()

    # Exit non-zero on staleness so the execution is also visibly failed in the
    # Cloud Run console, not just in the logs. The data is already safely
    # merged by this point, so a non-zero exit costs nothing.
    return 0 if fresh else 1


if __name__ == "__main__":
    sys.exit(main())
