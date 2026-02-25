#!/usr/bin/env python3

"""
BLS main runner: initialize bls_master.csv and sequentially call sub-scripts.

Responsibilities:
  - This script: create an empty structured bls_data/bls_master.csv
  - Sub-scripts: fetch their own metric and upsert into the existing master file

Usage:
    python3 bls_run.py --start 2022 --end 2025 --key YOUR_BLS_API_KEY
  or
    python bls_run.py --start 2022 --end 2025 --key YOUR_BLS_API_KEY
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import BLS_API_KEY


# config

BLS_DIR     = Path("data/bls_data")
MASTER_PATH = BLS_DIR / "bls_master.csv"

MASTER_COLUMNS = [
    "state", "state_abbr", "fips", "year", "month", "date",
    "unemployment_rate", "employment_rate", "avg_weekly_hours",
    "job_openings_rate", "quits_level_thousands",
    "avg_weekly_earnings",
]

# Sub-scripts: (script filename, metric column, single-metric output file)
SUB_SCRIPTS = [
    ("bls_unemployment_rate_50states.py",   "unemployment_rate",     "bls_unemployment_rate_50states.csv"),
    ("bls_employment_rate_50states.py",     "employment_rate",       "bls_employment_rate_50states.csv"),
    ("bls_avg_weekly_hours_50states.py",    "avg_weekly_hours",      "bls_avg_weekly_hours_50states.csv"),
    ("bls_job_opennings_rate_50states.py",  "job_openings_rate",     "bls_job_opennings_rate_50states.csv"),
    ("bls_quits_level_50states.py",         "quits_level_thousands", "bls_quits_level_50states.csv"),
    ("bls_avg_weekly_wage_50states.py",     "avg_weekly_earnings",   "bls_avg_weekly_wage_50states.csv"),
]


# Initialize master CSV

def init_master() -> None:
    empty = pd.DataFrame(columns=MASTER_COLUMNS)
    empty = empty.astype({
        "state":                 "object",
        "state_abbr":            "object",
        "fips":                  "object",
        "year":                  "Int64",
        "month":                 "Int64",
        "date":                  "object",
        "unemployment_rate":     "Float64",
        "employment_rate":       "Float64",
        "avg_weekly_hours":      "Float64",
        "job_openings_rate":     "Float64",
        "quits_level_thousands": "Float64",
        "avg_weekly_earnings":   "Float64",
    })

    BLS_DIR.mkdir(parents=True, exist_ok=True)
    empty.to_csv(MASTER_PATH, index=False)
    print(f"Created empty master: {MASTER_PATH}")


# Run sub-scripts sequentially
def run_sub_scripts(start: str, end: str, api_key: str | None) -> None:
    
    # get the path of the current script
    BLS_DIR.mkdir(parents=True, exist_ok=True)
    script_dir = Path(__file__).parent

    for script_name, metric, out_filename in SUB_SCRIPTS:
        # get the path of the sub-script
        script_path = script_dir / script_name

        # # check if the sub-script exists
        # if not script_path.exists():
        #     print(f"\n[ERROR] Sub-script not found: {script_path}")
        #     print("Ensure the following scripts are in the same directory:")
        #     for s, _, _ in SUB_SCRIPTS:
        #         print(f"  {s}")
        #     sys.exit(1)

        cmd = [
            sys.executable, str(script_path),
            "--start",  start,
            "--end",    end,
            "--master", str(MASTER_PATH),
            "--out",    str(BLS_DIR / out_filename),
        ]
        # allow use without an API key, but if provided, pass it to the sub-script
        if api_key:
            cmd += ["--key", api_key]


        result = subprocess.run(cmd)
        
        # if failed, print error and exit with the same code
        if result.returncode != 0:
            print(f"\n[ERROR] {script_name} exited with code {result.returncode}, aborting.")
            sys.exit(result.returncode)


    master_df = pd.read_csv(MASTER_PATH)
    print(f"Total rows: {len(master_df):,}")
    print(master_df.head(10).to_string(index=False))


# main method 
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize bls_master.csv and run all BLS data fetch scripts."
    )
    # add three args
    parser.add_argument("--start", required=True, help="Start year, e.g. 2022")
    parser.add_argument("--end",   required=True, help="End year,   e.g. 2025")
    parser.add_argument("--key",   default=None,  help="BLS API key (optional)")
    args = parser.parse_args()
    api_key = args.key or BLS_API_KEY or None

    init_master()
    run_sub_scripts(start=args.start, end=args.end, api_key=api_key)
    
if __name__ == "__main__":
    main()
