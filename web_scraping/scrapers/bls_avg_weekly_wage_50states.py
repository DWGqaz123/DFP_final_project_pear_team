#!/usr/bin/env python3
"""
Fetch 50-state Average Weekly Earnings (Total Private) from BLS CES State & Area (SMU) API v2.

Series ID format (statewide):
SMU + {state_fips} + 00000 + 05 + 000000 + 11
Example: California (06) => SMU06000000500000011

Run:
python3 bls_avg_weekly_earnings_50states.py --start 2022 --end 2024 --key 963729bfa50042e294f9e0516067fcb7 --out avg_weekly_earnings.csv
"""

import argparse
import time
import requests
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# The metric column name this script is responsible for
METRIC_COL = "avg_weekly_earnings"

STATE_FIPS = {
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
    "Wisconsin": "55", "Wyoming": "56"
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

# Master CSV columns — must stay in sync with bls_run.py
MASTER_COLUMNS = [
    "state", "state_abbr", "fips", "year", "month", "date",
    "unemployment_rate", "employment_rate", "avg_weekly_hours",
    "job_openings_rate", "quits_level_thousands",
    "avg_weekly_earnings",   # added by this script
]


def chunk_list(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]


def build_series_id(state_fips: str) -> str:
    # SMU + FIPS + 00000 (statewide) + 05 (Total Private) + 000000 (industry) + 11 (Avg Weekly Earnings)
    return f"SMU{state_fips}000000500000011"


def request_bls(series_ids, start_year, end_year, api_key=None, timeout=30):
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        payload["registrationkey"] = api_key

    resp = requests.post(BLS_URL, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse_bls_json(bls_json, sid_to_state):
    rows = []
    series_list = bls_json.get("Results", {}).get("series", [])
    for s in series_list:
        sid = s.get("seriesID")
        state = sid_to_state.get(sid, "UNKNOWN")

        for dp in s.get("data", []):
            period = dp.get("period")  # M01..M12, M13
            if not isinstance(period, str) or not period.startswith("M") or period == "M13":
                continue

            year = int(dp["year"])
            month = int(period[1:])
            try:
                value = float(dp["value"])
            except Exception:
                value = pd.NA

            rows.append({
                "state": state,
                "series_id": sid,
                "year": year,
                "month": month,
                METRIC_COL: value,
                "date": pd.Timestamp(year, month, 1),
            })

    return pd.DataFrame(rows)


def fetch_avg_weekly_earnings_50states(start_year, end_year, api_key=None, sleep_s=0.8):
    series_map = {build_series_id(fips): state for state, fips in STATE_FIPS.items()}
    series_ids = list(series_map.keys())

    # BLS: with key up to 50 series per request; without key up to 25
    batch_size = 50 if api_key else 25
    batches = chunk_list(series_ids, batch_size)

    parts = []
    for i, batch in enumerate(batches, 1):
        print(f"[{i}/{len(batches)}] Requesting {len(batch)} series...")
        bls_json = request_bls(batch, start_year, end_year, api_key=api_key)

        if bls_json.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(f"BLS request failed: {bls_json.get('message')}")

        part_df = parse_bls_json(bls_json, series_map)
        parts.append(part_df)

        time.sleep(sleep_s)

    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if df.empty:
        return df

    return df.sort_values(["state", "date"]).reset_index(drop=True)


def upsert_to_master(new_df: pd.DataFrame, master_path: str) -> None:
    """
    Upsert avg_weekly_earnings into the master CSV by (state, year, month).
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


def main():
    parser = argparse.ArgumentParser(description="Fetch 50-state Average Weekly Earnings (Total Private) from BLS SMU.")
    parser.add_argument("--start", required=True, help="Start year (e.g., 2022)")
    parser.add_argument("--end", required=True, help="End year (e.g., 2024)")
    parser.add_argument("--key", default=None, help="BLS API key (optional but recommended)")
    parser.add_argument("--master", default="bls_master.csv", help="Master CSV path for upsert")
    parser.add_argument("--out", default="avg_weekly_earnings.csv", help="Output CSV path")
    args = parser.parse_args()

    df = fetch_avg_weekly_earnings_50states(args.start, args.end, api_key=args.key)

    if df.empty:
        print("No data returned. Quick check: try one series like SMU06000000500000011 in Postman.")
        return

    df.to_csv(args.out, index=False)
    print(f"Saved {len(df):,} rows to {args.out}")
    print(df.head(10).to_string(index=False))

    upsert_to_master(df, master_path=args.master)


if __name__ == "__main__":
    main()
