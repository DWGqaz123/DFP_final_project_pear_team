#!/usr/bin/env python3
"""
main_run.py — ONE entry point for the whole project
"""

from __future__ import annotations
import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict
import pandas as pd
import numpy as np
import webbrowser
from config import BLS_API_KEY


# --- Paths / config ---
DATA_DIR = Path("data")
BLS_DIR = DATA_DIR / "bls_data"
BLS_MASTER_PATH = BLS_DIR / "bls_master.csv"
CRIME_CSV_PATH = DATA_DIR / "crime_rate_2022_2025_monthly.csv"
COST_TAX_CSV_PATH = DATA_DIR / "state_cost_tax_2025.csv"
MONTHLY_MERGED_PATH = DATA_DIR / "monthly_merged.csv"
DASHBOARD_JSON_PATH = DATA_DIR / "dashboard_data.json"
DASHBOARD_HTML_PATH = Path("dashboard.html")


# ---------------------------------------------------------
# STEP 1 — BLS Pipeline
# ---------------------------------------------------------
def run_bls(start: str, end: str, api_key: str | None) -> None:
    print("\n" + "=" * 60 + "\n[step 1] Running BLS pipeline\n" + "=" * 60)
    BLS_DIR.mkdir(parents=True, exist_ok=True)
    bls_script = Path("scrapers/bls_run.py")
    if not bls_script.exists():
        print(f"[ERROR] scrapers/bls_run.py not found")
        sys.exit(1)

    cmd = [sys.executable, str(bls_script), "--start", start, "--end", end]
    if api_key:
        cmd += ["--key", api_key]

    subprocess.run(cmd)


# ---------------------------------------------------------
# STEP 2 — Data Collecting
# ---------------------------------------------------------
def run_data_collecting() -> None:
    print("\n" + "=" * 60 + "\n[step 2] Running data collection\n" + "=" * 60)
    script = Path("scrapers/data_collecting.py")
    if not script.exists():
        print(f"[ERROR] scrapers/data_collecting.py not found")
        sys.exit(1)

    subprocess.run([sys.executable, str(script)])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for f in ["crime_rate_2022_2025_monthly.csv", "state_cost_tax_2025.csv"]:
        src = Path(f)
        if src.exists():
            src.replace(DATA_DIR / f)


# ---------------------------------------------------------
# STEP 3 — Merge Crime + BLS
# ---------------------------------------------------------
def parse_crime_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    df["_date"] = pd.to_datetime(df["Month"], errors="coerce")
    df["year"] = df["_date"].dt.year
    df["month"] = df["_date"].dt.month
    df["state_abbr"] = df["State_Abbreviation"].astype(str).str.strip().str.upper()
    df["violent_crime_rate_per_100k"] = pd.to_numeric(
        df["Violent_Crime_Rate_per_100k"], errors="coerce"
    )

    return df[
        ["state_abbr", "year", "month", "violent_crime_rate_per_100k"]
    ].dropna()


def merge_and_export() -> None:
    print("\n" + "=" * 60 + "\n[step 3] Merging datasets\n" + "=" * 60)

    bls = pd.read_csv(BLS_MASTER_PATH, parse_dates=["date"])
    bls["state_abbr"] = bls["state_abbr"].astype(str).str.strip().str.upper()

    if CRIME_CSV_PATH.exists():
        crime = parse_crime_csv(CRIME_CSV_PATH)
        merged = bls.merge(crime, on=["state_abbr", "year", "month"], how="left")
    else:
        merged = bls.copy()

    merged.sort_values(["state", "year", "month"]).to_csv(
        MONTHLY_MERGED_PATH, index=False
    )


# ---------------------------------------------------------
# STEP 4 — Compute Livability Rankings
# ---------------------------------------------------------
def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series(0.5, index=s.index)
    return (s - mn) / (mx - mn)


def compute_livability_rankings(
    monthly_merged_path: Path, cost_tax_path: Path, months_back: int, weights: Dict[str, float]
) -> pd.DataFrame:

    m = pd.read_csv(monthly_merged_path)
    ct = pd.read_csv(cost_tax_path)

    m["date"] = pd.to_datetime(m["date"], errors="coerce")

    recent = m[m["date"] >= (m["date"].max() - pd.DateOffset(months=months_back - 1))].copy()

    agg_cols = [
        "unemployment_rate",
        "employment_rate",
        "avg_weekly_earnings",
        "violent_crime_rate_per_100k",
    ]

    grp = recent.groupby("state", as_index=False)[agg_cols].mean()

    ct = ct.rename(columns={"State": "state"})
    grp = grp.merge(
        ct[["state", "Cost_of_Living_Index_2025", "Top_State_Income_Tax_Rate"]],
        on="state",
        how="left",
    )

    comps = {
        "unemp_good": 1 - _minmax(grp["unemployment_rate"]),
        "crime_good": 1 - _minmax(grp["violent_crime_rate_per_100k"]),
        "cost_good": 1 - _minmax(grp["Cost_of_Living_Index_2025"]),
        "tax_good": 1 - _minmax(grp["Top_State_Income_Tax_Rate"]),
        "employ_good": _minmax(grp["employment_rate"]),
        "wage_good": _minmax(grp["avg_weekly_earnings"]),
    }

    for k, v in comps.items():
        grp[k] = v

    w_sum = sum(weights.values()) or 1.0
    nw = {k: v / w_sum for k, v in weights.items()}

    grp["livability_score"] = (
        nw["w_cost"] * grp["cost_good"] +
        nw["w_safety"] * grp["crime_good"] +
        nw["w_tax"] * grp["tax_good"] +
        nw["w_unemp"] * grp["unemp_good"] +
        nw["w_employ"] * grp["employ_good"] +
        nw["w_wage"] * grp["wage_good"]
    )

    grp["livability_score"] = grp["livability_score"].fillna(0)

    grp["rank"] = grp["livability_score"].rank(
        ascending=False,
        method="dense"
    ).astype(int)

    return grp.sort_values("rank")


# ---------------------------------------------------------
# STEP 5 — Build Dashboard (JSON only)
# ---------------------------------------------------------
def build_dashboard(rankings_df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    clean_df = rankings_df.replace({np.nan: None})
    json.dump(
        clean_df.to_dict(orient="records"),
        open(DASHBOARD_JSON_PATH, "w"),
        indent=2,
    )

    # IMPORTANT: Do NOT overwrite dashboard.html anymore
    print("[Dashboard] JSON updated at:", DASHBOARD_JSON_PATH)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="ONE entry point: pipeline + analysis + dashboard."
    )

    parser.add_argument("--start", default="2022")
    parser.add_argument("--end", default="2025")
    parser.add_argument("--key", default=None)
    parser.add_argument("--use-existing", action="store_true")
    parser.add_argument("--auto", action="store_true")

    args = parser.parse_args()
    api_key = args.key or BLS_API_KEY or None

    if args.auto:
        if not args.use_existing:
            run_bls(args.start, args.end, api_key)
            run_data_collecting()
            merge_and_export()

        w = {
            "w_cost": 0.20,
            "w_safety": 0.18,
            "w_tax": 0.10,
            "w_unemp": 0.18,
            "w_employ": 0.12,
            "w_wage": 0.22,
        }

        rankings = compute_livability_rankings(
            MONTHLY_MERGED_PATH, COST_TAX_CSV_PATH, 12, w
        )

        build_dashboard(rankings)
        print("\n[DONE] Dashboard updated.")

if __name__ == "__main__":
    main()
