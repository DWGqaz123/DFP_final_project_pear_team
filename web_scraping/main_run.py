#!/usr/bin/env python3
"""
Master pipeline script.

merge the outcome of following 2 scripts:
- bls_run.py: fetches and processes BLS data
- data_collecting.py: fetches crime and cost-of-living data

use:
  python run_pipeline.py --start 2022 --end 2025 --key 963729bfa50042e294f9e0516067fcb7
  or
  python3 run_pipeline.py --start 2022 --end 2025 --key 963729bfa50042e294f9e0516067fcb7
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


# config

DATA_DIR        = Path("data")
BLS_DIR         = DATA_DIR / "bls_data"          # all BLS files go here

BLS_MASTER_PATH     = BLS_DIR  / "bls_master.csv" # all BLS data (monthly)
CRIME_CSV_PATH      = DATA_DIR / "crime_rate_2022_2025_monthly.csv" # crime data (monthly)
COST_TAX_CSV_PATH   = DATA_DIR / "state_cost_tax_2025.csv" # cost-of-living and tax data(yearly)
MONTHLY_MERGED_PATH = DATA_DIR / "monthly_merged.csv" # final merged output: BLS + crime (monthly)



# Run the BLS pipeline (bls_run.py)
def run_bls(start: str, end: str, api_key: str | None) -> None:
    print("\n" + "="*60)
    print("[step 1] Running BLS pipeline (bls_run.py)")
    print("="*60)

    BLS_DIR.mkdir(parents=True, exist_ok=True)

    # check if bls_master.csv exists, if not, create an empty one with the right columns
    bls_script = Path("scrapers/bls_run.py")
    if not bls_script.exists():
        print(f"[ERROR] bls_run.py not found in {Path.cwd()}")
        print(f"Files in current directory: {list(Path('.').glob('*.py'))}")
        sys.exit(1)

    # run the bls_run.py
    cmd = [
        sys.executable, str(bls_script),
        "--start",  start,
        "--end",    end
    ]
    if api_key:
        cmd += ["--key", api_key]

    # print the command being run for clarity
    print(f"[step 1] Running command: {' '.join(cmd)}")

    
    result = subprocess.run(cmd)
    # print the return code for debugging
    print(f"[step 1] bls_run.py exited with code: {result.returncode}")
    
    if result.returncode != 0:
        print("[ERROR] bls_run.py failed.")
        sys.exit(result.returncode)



# Run the data collecting pipeline (data_collecting.py)

def run_data_collecting() -> None:
    print("\n" + "="*60)
    print("[step 2] Running data collection (data_collecting.py)")
    print("="*60)

    result = subprocess.run([sys.executable, "scrapers/data_collecting.py"])
    if result.returncode != 0:
        print("[ERROR] data_collecting.py failed.")
        sys.exit(result.returncode)

    # Move outputs into data/ if they landed in the project root
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for filename in ["crime_rate_2022_2025_monthly.csv", "state_cost_tax_2025.csv"]:
        src = Path(filename)
        dst = DATA_DIR / filename
        if src.exists():
            src.replace(dst)
            print(f"[step 2] Moved {src} -> {dst}")
        elif dst.exists():
            print(f"[step 2] {dst} already in place.")
        else:
            print(f"[WARNING] {filename} not found after data_collecting.py ran.")




# parse crime CSV into a clean format for merging with BLS data
def parse_crime_csv(path: Path) -> pd.DataFrame:
    """
    Input columns: State, State_Abbreviation, Month, Violent_Crime_Rate_per_100k
    Output columns: state, year, month, violent_crime_rate_per_100k
    """
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    df["_date"] = pd.to_datetime(df["Month"]) 
    df["year"]  = df["_date"].dt.year
    df["month"] = df["_date"].dt.month

    df = df.rename(columns={
        "State":                       "state",
        "Violent_Crime_Rate_per_100k": "violent_crime_rate_per_100k",
    })

    return df[["state", "year", "month", "violent_crime_rate_per_100k"]]



# [step 3]Merge and save outcome

def merge_and_export() -> None:
    print("\n" + "="*60)
    print("[step 3] Merging bls_master + crime data -> monthly_merged.csv")
    print("="*60)

    bls = pd.read_csv(BLS_MASTER_PATH, parse_dates=["date"])

    if CRIME_CSV_PATH.exists():
        crime  = parse_crime_csv(CRIME_CSV_PATH)
        merged = bls.merge(crime, on=["state", "year", "month"], how="left")
        print(f"[step 3] Crime data merged.")
    
    # in case if crime CSV is missing, just output the BLS data with a warning 
    else:
        merged = bls.copy()
        print(f"[step 3] Crime CSV not found, skipping merge. Outputting BLS data only.")

    merged = merged.sort_values(["state", "year", "month"]).reset_index(drop=True)
    merged.to_csv(MONTHLY_MERGED_PATH, index=False)
    
    print(f"[step 3] -> {MONTHLY_MERGED_PATH}  ({len(merged):,} rows)")
    print(merged.head(10).to_string(index=False))



# main method to run the whole pipeline
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Master pipeline: BLS + crime/cost data -> final merged CSVs."
    )
    parser.add_argument("--start", required=True, help="BLS start year, e.g. 2022")
    parser.add_argument("--end",   required=True, help="BLS end year,   e.g. 2024")
    parser.add_argument("--key",   default=None,  help="BLS API key (optional)")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: BLS -> data/bls_data/
    run_bls(args.start, args.end, args.key)

    # Step 2: crime + cost-of-living -> data/
    run_data_collecting()

    # Step 3: merge -> data/monthly_merged.csv
    merge_and_export()

    print("\n" + "="*60)
    print("[done] Pipeline complete. Final output files:")
    print(f"  {MONTHLY_MERGED_PATH}")
    print(f"  {COST_TAX_CSV_PATH}")
    print("="*60)


if __name__ == "__main__":
    main()