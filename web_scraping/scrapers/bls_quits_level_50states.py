#!/usr/bin/env python3
"""
Fetch US state quits level from BLS Public Data API v2 (JOLTS).

Series ID structure: JTU{industry}{FIPS}{area}{size}{data_type}
  Example: JTU000000060000000QUL (California)
    - JT        : JOLTS series prefix
    - U         : Not seasonally adjusted
    - 000000    : Total nonfarm, all industries
    - 06        : State FIPS code
    - 00000     : Statewide
    - 00        : All establishment sizes
    - QUL       : Quits Level (in thousands)

Requirements:
  pip install requests pandas

Usage:
  python bls_quits_50states.py --start 2022 --end 2025 --key YOUR_BLS_API_KEY --out quits.csv
  python bls_quits_50states.py --start 2022 --end 2025 --out quits.csv 
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
import pandas as pd



BLS_V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

METRIC_COL = "quits_level_thousands"

STATE_FIPS: Dict[str, str] = {
    "Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05",
    "California": "06", "Colorado": "08", "Connecticut": "09",
    "Delaware": "10", "Florida": "12", "Georgia": "13",
    "Hawaii": "15", "Idaho": "16", "Illinois": "17",
    "Indiana": "18", "Iowa": "19", "Kansas": "20",
    "Kentucky": "21", "Louisiana": "22", "Maine": "23",
    "Maryland": "24", "Massachusetts": "25", "Michigan": "26",
    "Minnesota": "27", "Mississippi": "28", "Missouri": "29",
    "Montana": "30", "Nebraska": "31", "Nevada": "32",
    "New Hampshire": "33", "New Jersey": "34", "New Mexico": "35",
    "New York": "36", "North Carolina": "37", "North Dakota": "38",
    "Ohio": "39", "Oklahoma": "40", "Oregon": "41",
    "Pennsylvania": "42", "Rhode Island": "44",
    "South Carolina": "45", "South Dakota": "46",
    "Tennessee": "47", "Texas": "48", "Utah": "49",
    "Vermont": "50", "Virginia": "51",
    "Washington": "53", "West Virginia": "54",
    "Wisconsin": "55", "Wyoming": "56",
}

STATE_ABBR: Dict[str, str] = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
    "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA",
    "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}

# Master CSV columns — must stay in sync with bls_run_all.py
MASTER_COLUMNS = [
    "state", "state_abbr", "fips", "year", "month", "date",
    "unemployment_rate", "employment_rate", "avg_weekly_hours",
    "job_openings_rate", "quits_level_thousands",
]


# Config

@dataclass
class BLSConfig:
    start_year: str
    end_year: str
    api_key: Optional[str] = None
    timeout_s: int = 30
    max_retries: int = 3
    sleep_between_calls_s: float = 0.8


# Helpers

def make_series_id(fips: str) -> str:
    """Build JOLTS not-seasonally-adjusted quits level series ID from state FIPS."""
    return f"JTU000000{fips}0000000QUL"


def build_series_lookup() -> Dict[str, str]:
    """Return a dict mapping series_id -> state name."""
    return {make_series_id(fips): state for state, fips in STATE_FIPS.items()}


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def safe_post_json(url: str, payload: dict, timeout_s: int, max_retries: int) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=payload, timeout=timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(f"Failed after {max_retries} retries. Last error: {last_err}")


# Fetch

def parse_bls_response(
    bls_json: dict,
    series_to_state: Dict[str, str],
) -> pd.DataFrame:
    """
    Flatten BLS response into tidy rows:
    state, year, month, period, period_name, date, quits_level_thousands
    """
    if "Results" not in bls_json or "series" not in bls_json["Results"]:
        raise ValueError(f"Unexpected BLS response format: {bls_json.keys()}")

    rows: List[dict] = []
    for series_obj in bls_json["Results"]["series"]:
        sid = series_obj.get("seriesID")
        state = series_to_state.get(sid, "UNKNOWN")

        for dp in series_obj.get("data", []):
            period = dp.get("period", "")
            # Keep monthly only (M01..M12), drop M13 annual average
            if not period.startswith("M") or period == "M13":
                continue
            year = int(dp["year"])
            month = int(period[1:])
            try:
                value = float(dp["value"])
            except Exception:
                value = pd.NA
            rows.append({
                "state":       state,
                "year":        year,
                "month":       month,
                "period":      period,
                "period_name": dp.get("periodName"),
                "date":        pd.Timestamp(year, month, 1),
                METRIC_COL:    value,
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["state", "date"]).reset_index(drop=True)
    return df


def fetch_quits_50_states(cfg: BLSConfig) -> pd.DataFrame:
    # BLS limits: registered up to 50 series/query; unregistered up to 25
    batch_size = 50 if cfg.api_key else 25

    series_to_state = build_series_lookup()
    series_ids = list(series_to_state.keys())
    batches = chunk_list(series_ids, batch_size)
    all_parts: List[pd.DataFrame] = []

    for idx, batch in enumerate(batches, start=1):
        payload: dict = {
            "seriesid":  batch,
            "startyear": cfg.start_year,
            "endyear":   cfg.end_year,
        }
        if cfg.api_key:
            payload["registrationkey"] = cfg.api_key

        print(f"[{idx}/{len(batches)}] Requesting {len(batch)} series...")
        bls_json = safe_post_json(BLS_V2_URL, payload, cfg.timeout_s, cfg.max_retries)

        status = bls_json.get("status")
        if status != "REQUEST_SUCCEEDED":
            raise RuntimeError(
                f"BLS request failed: status={status}, "
                f"message={bls_json.get('message')}"
            )

        all_parts.append(parse_bls_response(bls_json, series_to_state))
        time.sleep(cfg.sleep_between_calls_s)

    if not all_parts:
        return pd.DataFrame()
    return pd.concat(all_parts, ignore_index=True)


# Upsert to master CSV

def upsert_to_master(new_df: pd.DataFrame, master_path: str) -> None:
    """
    Upsert quits_level_thousands into the master CSV by (state, year, month).
    Silently skips if the master file does not exist.
    """
    master_file = Path(master_path)
    if not master_file.exists():
        print(f"[master] {master_path} not found, skipping upsert.")
        return

    master = pd.read_csv(master_file, parse_dates=["date"])
    master = master.drop(columns=[METRIC_COL], errors="ignore")

    incoming = (
        new_df[["state", "year", "month", METRIC_COL]]
        .dropna(subset=METRIC_COL) # pyright: ignore[reportCallIssue]
        .drop_duplicates(subset=["state", "year", "month"])
        .copy()
    )

    merged = master.merge(incoming, on=["state", "year", "month"], how="outer")

    merged["state_abbr"] = merged["state"].map(STATE_ABBR)
    merged["fips"]       = merged["state"].map(STATE_FIPS)
    merged["date"]       = pd.to_datetime(
        merged[["year", "month"]].assign(day=1)
    )

    extra_cols = [c for c in merged.columns if c not in MASTER_COLUMNS]
    final_cols = [c for c in MASTER_COLUMNS if c in merged.columns] + extra_cols
    merged = merged[final_cols].sort_values(["state", "year", "month"]).reset_index(drop=True)

    merged.to_csv(master_path, index=False)
    print(f"[master] {METRIC_COL} upsert complete: {len(merged):,} rows -> {master_path}")


# main

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch 50-state quits level (JOLTS, not seasonally adjusted) from BLS API v2."
    )
    parser.add_argument("--start",  required=True, help="Start year, e.g. 2022")
    parser.add_argument("--end",    required=True, help="End year, e.g. 2024")
    parser.add_argument("--key",    default=None,  help="Optional BLS registration key")
    parser.add_argument("--out",    default="bls_quits_50states.csv", help="Output CSV path")
    parser.add_argument("--master", default="bls_master.csv", help="collet all the data into this master CSV, with upsert logic")
    args = parser.parse_args()

    cfg = BLSConfig(start_year=str(args.start), end_year=str(args.end), api_key=args.key)

    df = fetch_quits_50_states(cfg)
    if df.empty:
        print("No data returned.")
        return

    # Save single-metric CSV
    df.to_csv(args.out, index=False)
    print(f"Saved {len(df):,} rows to {args.out}")
    print(df.head(10).to_string(index=False))

    # Upsert to master
    upsert_to_master(df, master_path=args.master)


if __name__ == "__main__":
    main()