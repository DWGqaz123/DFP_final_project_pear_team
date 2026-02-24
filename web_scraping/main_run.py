#!/usr/bin/env python3
"""
main_run.py — ONE entry point for the whole project

What this file does (matches instructor requirement for ONE main program file):
1) (Optional) Run data collection pipeline
   - BLS API pipeline via scrapers/bls_run.py
   - Supplemental data via scrapers/data_collecting.py (crime + cost of living + tax)
   - Merge into data/monthly_merged.csv
2) Run analysis on collected CSVs
   - Compute a weighted "livability_score" per state (more livable = higher score)
   - Export rankings to data/livability_rankings.csv
3) Build a modern, minimal web dashboard (yellow-brown theme)
   - data/dashboard_data.json
   - data/dashboard.html

Typical usage:
- Full pipeline (may hit anti-scraping / network issues):
  python main_run.py --start 2022 --end 2025 --key YOUR_BLS_KEY --build-dashboard

- Use cached CSVs only (recommended for stable demo / grading):
  python main_run.py --use-existing --build-dashboard

Weight tuning example:
  python main_run.py --use-existing --build-dashboard --w_cost 0.25 --w_safety 0.20 --w_wage 0.25
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------

DATA_DIR = Path("data")
BLS_DIR = DATA_DIR / "bls_data"

BLS_MASTER_PATH = BLS_DIR / "bls_master.csv"
CRIME_CSV_PATH = DATA_DIR / "crime_rate_2022_2025_monthly.csv"
COST_TAX_CSV_PATH = DATA_DIR / "state_cost_tax_2025.csv"
MONTHLY_MERGED_PATH = DATA_DIR / "monthly_merged.csv"

RANKINGS_CSV_PATH = DATA_DIR / "livability_rankings.csv"
DASHBOARD_JSON_PATH = DATA_DIR / "dashboard_data.json"
DASHBOARD_HTML_PATH = DATA_DIR / "dashboard.html"


# ---------------------------------------------------------------------
# Step 1: Run BLS pipeline (scrapers/bls_run.py)
# ---------------------------------------------------------------------

def run_bls(start: str, end: str, api_key: str | None) -> None:
    print("\n" + "=" * 60)
    print("[step 1] Running BLS pipeline (scrapers/bls_run.py)")
    print("=" * 60)

    BLS_DIR.mkdir(parents=True, exist_ok=True)

    bls_script = Path("scrapers/bls_run.py")
    if not bls_script.exists():
        print(f"[ERROR] scrapers/bls_run.py not found in {Path.cwd()}")
        sys.exit(1)

    cmd = [sys.executable, str(bls_script), "--start", start, "--end", end]
    if api_key:
        cmd += ["--key", api_key]

    print(f"[step 1] Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    print(f"[step 1] bls_run.py exited with code: {result.returncode}")
    if result.returncode != 0:
        print("[ERROR] BLS pipeline failed.")
        sys.exit(result.returncode)


# ---------------------------------------------------------------------
# Step 2: Run scraping / API pipeline (scrapers/data_collecting.py)
# ---------------------------------------------------------------------

def run_data_collecting() -> None:
    print("\n" + "=" * 60)
    print("[step 2] Running data collection (scrapers/data_collecting.py)")
    print("=" * 60)

    script = Path("scrapers/data_collecting.py")
    if not script.exists():
        print(f"[ERROR] scrapers/data_collecting.py not found in {Path.cwd()}")
        sys.exit(1)

    result = subprocess.run([sys.executable, str(script)])
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


# ---------------------------------------------------------------------
# Step 3: Merge BLS + crime -> data/monthly_merged.csv
# ---------------------------------------------------------------------

def parse_crime_csv(path: Path) -> pd.DataFrame:
    """
    Input columns (expected): State, State_Abbreviation, Month, Violent_Crime_Rate_per_100k
    Output columns: state_abbr, year, month, violent_crime_rate_per_100k
    """
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    # robust date parsing
    df["_date"] = pd.to_datetime(df["Month"], errors="coerce")
    df["year"] = df["_date"].dt.year
    df["month"] = df["_date"].dt.month

    # Prefer abbreviation as merge key (more stable than full state name)
    if "State_Abbreviation" not in df.columns:
        raise KeyError(
            "Crime CSV missing 'State_Abbreviation'. "
            "Please check crime_rate_2022_2025_monthly.csv columns."
        )

    df["state_abbr"] = df["State_Abbreviation"].astype(str).str.strip().str.upper()

    # numeric
    if "Violent_Crime_Rate_per_100k" not in df.columns:
        raise KeyError(
            "Crime CSV missing 'Violent_Crime_Rate_per_100k'. "
            "Please check crime_rate_2022_2025_monthly.csv columns."
        )

    df["violent_crime_rate_per_100k"] = pd.to_numeric(
        df["Violent_Crime_Rate_per_100k"], errors="coerce"
    )

    out = df[["state_abbr", "year", "month", "violent_crime_rate_per_100k"]].copy()

    # drop rows that cannot be merged
    out = out.dropna(subset=["state_abbr", "year", "month"])
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)

    return out


def merge_and_export() -> None:
    print("\n" + "=" * 60)
    print("[step 3] Merging bls_master + crime data -> monthly_merged.csv")
    print("=" * 60)

    if not BLS_MASTER_PATH.exists():
        raise FileNotFoundError(f"Missing {BLS_MASTER_PATH}. Run BLS pipeline first.")

    bls = pd.read_csv(BLS_MASTER_PATH, parse_dates=["date"])
    bls.columns = [c.strip() for c in bls.columns]

    # Make sure state_abbr exists in BLS master
    if "state_abbr" not in bls.columns:
        raise KeyError(
            "BLS master missing 'state_abbr'. "
            "Please check scrapers/bls_run.py output columns."
        )

    # normalize BLS key
    bls["state_abbr"] = bls["state_abbr"].astype(str).str.strip().str.upper()

    if CRIME_CSV_PATH.exists():
        crime = parse_crime_csv(CRIME_CSV_PATH)

        # IMPORTANT: merge on state_abbr + year + month (stable key)
        merged = bls.merge(crime, on=["state_abbr", "year", "month"], how="left")
        print("[step 3] Crime data merged (key = state_abbr, year, month).")

        # quick validation
        hit_rate = merged["violent_crime_rate_per_100k"].notna().mean() * 100.0
        print(f"[step 3] Merge hit-rate (non-null violent_crime_rate_per_100k): {hit_rate:.1f}%")
    else:
        merged = bls.copy()
        print("[step 3] Crime CSV not found, skipping crime merge.")

    merged = merged.sort_values(["state", "year", "month"]).reset_index(drop=True)
    merged.to_csv(MONTHLY_MERGED_PATH, index=False)

    print(f"[step 3] -> {MONTHLY_MERGED_PATH}  ({len(merged):,} rows)")
    print(merged.head(10).to_string(index=False))


# ---------------------------------------------------------------------
# Step 4: Analysis — weighted livability ranking
# ---------------------------------------------------------------------

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([0.5] * len(s), index=s.index)
    return (s - mn) / (mx - mn)


def compute_livability_rankings(
    monthly_merged_path: Path,
    cost_tax_path: Path,
    months_back: int,
    weights: Dict[str, float],
) -> pd.DataFrame:
    """
    Compute a per-state livability score from multiple sources.

    Approach:
    - Take the most recent N months (default 12)
    - Aggregate each metric by state (mean)
    - Normalize each metric to 0–1 (min-max)
    - Combine into a weighted score (higher is better)

    Metrics (typical):
      Positive (higher is better): employment_rate, avg_weekly_earnings
      Negative (lower is better): unemployment_rate, violent_crime_rate_per_100k,
                                  Cost_of_Living_Index_2025, Top_State_Income_Tax_Rate
    """
    m = pd.read_csv(monthly_merged_path)
    ct = pd.read_csv(cost_tax_path)

    # Ensure date
    if "date" in m.columns:
        m["date"] = pd.to_datetime(m["date"], errors="coerce")
    else:
        m["date"] = pd.to_datetime(m[["year", "month"]].assign(day=1), errors="coerce")

    # Filter recent months
    max_date = m["date"].max()
    if pd.isna(max_date):
        raise ValueError("monthly_merged.csv has no valid dates.")
    cutoff = (max_date - pd.DateOffset(months=months_back - 1)).to_period("M").to_timestamp()
    recent = m[m["date"] >= cutoff].copy()

    # Aggregate by state
    agg_cols = []
    for c in [
        "unemployment_rate",
        "employment_rate",
        "avg_weekly_earnings",
        "violent_crime_rate_per_100k",
    ]:
        if c in recent.columns:
            recent[c] = pd.to_numeric(recent[c], errors="coerce")
            agg_cols.append(c)

    grp = recent.groupby("state", as_index=False)[agg_cols].mean(numeric_only=True)

    # Join cost/tax (yearly)
    # expected columns in your scraped csv: State, State_Abbreviation, Cost_of_Living_Index_2025, Top_State_Income_Tax_Rate
    ct2 = ct.rename(columns={"State": "state"}).copy()
    for c in ["Cost_of_Living_Index_2025", "Top_State_Income_Tax_Rate"]:
        if c in ct2.columns:
            ct2[c] = pd.to_numeric(ct2[c], errors="coerce")

    grp = grp.merge(
        ct2[["state", "Cost_of_Living_Index_2025", "Top_State_Income_Tax_Rate"]],
        on="state",
        how="left",
    )

    # Normalize components
    comps: Dict[str, pd.Series] = {}

    # Negative metrics -> convert to "goodness" by (1 - norm)
    if "unemployment_rate" in grp.columns:
        comps["unemp_good"] = 1 - _minmax(grp["unemployment_rate"])
    if "violent_crime_rate_per_100k" in grp.columns:
        comps["crime_good"] = 1 - _minmax(grp["violent_crime_rate_per_100k"])
    if "Cost_of_Living_Index_2025" in grp.columns:
        comps["cost_good"] = 1 - _minmax(grp["Cost_of_Living_Index_2025"])
    if "Top_State_Income_Tax_Rate" in grp.columns:
        comps["tax_good"] = 1 - _minmax(grp["Top_State_Income_Tax_Rate"])

    # Positive metrics
    if "employment_rate" in grp.columns:
        comps["employ_good"] = _minmax(grp["employment_rate"])
    if "avg_weekly_earnings" in grp.columns:
        comps["wage_good"] = _minmax(grp["avg_weekly_earnings"])

    comp_df = pd.DataFrame(comps)
    # attach normalized components back to grp so frontend can re-weight
    for col in comp_df.columns:
        grp[col] = comp_df[col].values

    # Weight keys -> comp columns mapping
    wmap = {
        "w_cost": "cost_good",
        "w_safety": "crime_good",
        "w_tax": "tax_good",
        "w_unemp": "unemp_good",
        "w_employ": "employ_good",
        "w_wage": "wage_good",
    }
    used = []
    w_used = []
    for wk, ck in wmap.items():
        if ck in comp_df.columns and wk in weights:
            used.append(ck)
            w_used.append(float(weights[wk]))

    if not used:
        raise ValueError("No components available to compute score. Check input columns.")

    # Normalize weights among used components
    total_w = sum(w_used)
    if total_w <= 0:
        w_used = [1.0] * len(w_used)
        total_w = float(len(w_used))
    w_norm = [w / total_w for w in w_used]

    score = pd.Series(0.0, index=comp_df.index)
    for ck, w in zip(used, w_norm):
        score += comp_df[ck].fillna(0.5) * w

    grp["livability_score"] = score
    grp["rank"] = grp["livability_score"].rank(ascending=False, method="min").astype(int)
    grp = grp.sort_values(["rank", "state"]).reset_index(drop=True)

    # Keep a clean output
    out_cols = [
        "rank",
        "state",
        "livability_score",
        "Cost_of_Living_Index_2025",
        "Top_State_Income_Tax_Rate",
        "unemployment_rate",
        "employment_rate",
        "avg_weekly_earnings",
        "violent_crime_rate_per_100k",
        "cost_good",
        "crime_good",
        "tax_good",
        "unemp_good",
        "employ_good",
        "wage_good",
    ]
    out_cols = [c for c in out_cols if c in grp.columns]
    return grp[out_cols]


# ---------------------------------------------------------------------
# Step 5: Build dashboard (static HTML + JSON, no f-string conflicts)
# ---------------------------------------------------------------------

def build_dashboard(rankings_df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ⭐ 把 NaN 轉成 None (→ JSON 變成 null)
    clean_df = rankings_df.replace({np.nan: None})

    rankings = clean_df.to_dict(orient="records")

    with open(DASHBOARD_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rankings, f, ensure_ascii=False, indent=2, allow_nan=False)

    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pear Team — State Livability Dashboard</title>
<style>
  :root{
    --bg: #f6f2e8;
    --card: #ffffff;
    --text: #2f2416;
    --muted: #6b5a43;
    --accent: #c9a66b;     /* warm gold */
    --accent2:#a67c52;     /* brown */
    --line: rgba(47,36,22,0.10);
    --shadow: 0 10px 35px rgba(0,0,0,0.08);
    --radius: 16px;
  }
  *{ box-sizing: border-box; }
  body{
    margin:0;
    font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, Arial, sans-serif;
    background: radial-gradient(900px 380px at 20% 0%, rgba(201,166,107,0.25), transparent 60%),
                radial-gradient(700px 320px at 90% 10%, rgba(166,124,82,0.18), transparent 55%),
                var(--bg);
    color: var(--text);
  }
  .wrap{
    max-width: 1280px;
    margin: 0 auto;
    padding: 32px 28px 70px;
  }
  header{
    display:flex; align-items:flex-end; justify-content:space-between; gap:16px;
    margin-bottom:18px;
  }
  h1{ margin:0; font-weight: 700; letter-spacing:-0.02em; font-size: 26px;}
  .subtitle{ margin:6px 0 0; color: var(--muted); font-size: 13px; line-height:1.4;}
  .pill{
    padding:10px 12px; border-radius:999px;
    border:1px solid var(--line);
    background: rgba(255,255,255,0.65);
    box-shadow: var(--shadow);
    color: var(--muted);
    font-size:12px;
    user-select:none;
  }
  .grid{
    display:grid;
    grid-template-columns: 1.2fr 0.8fr;
    gap:18px;
    align-items:start;
  }
  .card{
    background: var(--card);
    border:1px solid var(--line);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    overflow:hidden;
  }
  .card .hd{
    padding:14px 16px;
    border-bottom:1px solid var(--line);
    display:flex; align-items:center; justify-content:space-between;
  }
  .card .hd h2{
    margin:0; font-size:14px; letter-spacing:-0.01em;
  }
  .card .bd{ 
    position: relative;
    isolation: isolate;
  }
  table{
    width:100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th, td{
    padding: 10px 10px;
    border-bottom: 1px solid rgba(47,36,22,0.08);
    text-align:left;
  }
  th{
    background: linear-gradient(180deg, #c9a66b, #bf995d);
    color: #fff;
    font-weight: 700;
    position: sticky;
    top: 0;
    z-index: 20;                 
    border-bottom: 2px solid rgba(0,0,0,0.08);
  }
  tbody::before{
  content: "";
  display: block;
  height: 44px;
}
  tbody tr:hover td{ background: rgba(201,166,107,0.12); }
  .score{ font-weight:800; color: var(--accent2); }
  .controls{
    display:flex; gap:10px; flex-wrap:wrap; align-items:center;
    margin-bottom: 10px;
  }
  select, input{
    padding: 10px 12px;
    border-radius: 12px;
    border: 1px solid rgba(47,36,22,0.18);
    background: rgba(255,255,255,0.85);
    color: var(--text);
    font-size: 13px;
    outline:none;
    min-width: 200px;
  }
  .kpis{
    display:grid;
    grid-template-columns: 1fr 1fr;
    gap:10px;
    margin-top: 8px;
  }
  .kpi{
    border:1px solid var(--line);
    border-radius: 14px;
    padding: 12px 12px;
    background: rgba(246,242,232,0.55);
  }
  .kpi .lbl{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
  .kpi .val{ font-size: 16px; font-weight: 800; letter-spacing:-0.01em; }
  .kpi .sub{ margin-top: 6px; font-size: 12px; color: var(--muted); }
  .footer{
    margin-top: 12px;
    color: var(--muted);
    font-size: 12px;
    line-height: 1.5;
  }
  .weights{
    padding: 0 12px 12px;
  }
  .wrow{
    display:grid;
    grid-template-columns: 150px 1fr 56px;
    gap:10px;
    align-items:center;
    margin: 10px 0;
  }
  .wlabel{ color: var(--muted); font-size: 12px; }
  .wval{ text-align:right; color: var(--text); font-weight:700; font-size:12px; }
  .weights input[type="range"]{
    width:100%;
  }
  @media (max-width: 920px){
    .wrap{ padding: 22px 18px 50px; }
    .grid{ grid-template-columns: 1fr; }
    .card{ border-radius: 18px; }
    .card .hd{ padding: 14px; }
    .card .bd{ padding: 14px; } 
    .kpis{ grid-template-columns: 1fr; }
    select, input{ min-width: 160px; }
  }
  /* Ensure the weights area is actually clickable/draggable */
.weights, .wrow, .wrow *{
  pointer-events: auto !important;
}

.weights{
  position: relative;
  z-index: 50;            /* lift above any overlay */
}

/* Make the slider thicker and thumb easier to grab */
.weights input[type="range"]{
  -webkit-appearance: none;
  appearance: none;
  width: 100%;
  height: 10px;
  border-radius: 999px;
  background: rgba(47,36,22,0.18);
  outline: none;
  cursor: grab;
  touch-action: pan-x;    /* important for trackpad/touch drag */
}

.weights input[type="range"]:active{
  cursor: grabbing;
}

.weights input[type="range"]::-webkit-slider-thumb{
  -webkit-appearance: none;
  appearance: none;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: #fff;
  border: 2px solid rgba(166,124,82,0.9);
  box-shadow: 0 6px 16px rgba(0,0,0,0.14);
}

.weights input[type="range"]::-moz-range-thumb{
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: #fff;
  border: 2px solid rgba(166,124,82,0.9);
  box-shadow: 0 6px 16px rgba(0,0,0,0.14);
}
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>🏆 State Livability Dashboard</h1>
        <div class="subtitle">Weighted ranking built from BLS labor market metrics + FBI crime rate + scraped cost-of-living & income tax.</div>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="hd"><h2>Top 25 States</h2></div>
        <div class="bd" style="max-height:520px; overflow:auto;">
          <table>
            <thead>
              <tr>
                <th>Rank</th>
                <th>State</th>
                <th>Score</th>
                <th>Cost Index</th>
                <th>Tax Rate</th>
              </tr>
            </thead>
            <tbody id="rankTableBody"></tbody>
          </table>
          <div class="footer">Tip: Click a row to view details on the right.</div>
        </div>
      </div>

      <div class="card">
        <div class="hd"><h2>State Detail</h2></div>
        <div class="bd">
          <div class="controls">
            <select id="stateSelect"></select>
            <input id="searchBox" placeholder="Quick filter (e.g., Tex)"/>
          </div>

          <div class="weights" id="weightsPanel"></div>
          <div class="footer">Adjust weights to re-rank states in real time. Weights are auto-normalized.</div>


          <div class="kpis" id="kpis"></div>

        </div>
      </div>
    </div>
  </div>

<script>
fetch("dashboard_data.json")
  .then(resp => resp.json())
  .then(data => {
    let rows = data; // will be re-sorted based on weights
    const tbody = document.getElementById("rankTableBody");
    const select = document.getElementById("stateSelect");
    const search = document.getElementById("searchBox");
    const weightsPanel = document.getElementById("weightsPanel");

    // default weights (same idea as python)
    const DEFAULT_W = {
      w_cost: 0.20,
      w_safety: 0.18,
      w_tax: 0.10,
      w_unemp: 0.18,
      w_employ: 0.12,
      w_wage: 0.22
    };

    // load saved weights if any
    let W = (() => {
      try {
        const saved = JSON.parse(localStorage.getItem("livability_weights") || "null");
        return saved ? { ...DEFAULT_W, ...saved } : { ...DEFAULT_W };
      } catch(e){
        return { ...DEFAULT_W };
      }
    })();

    function fmt(x, digits=2){
      if (x === null || x === undefined || x === "") return "—";
      if (typeof x === "number") return x.toFixed(digits);
      return String(x);
    }

    function normalizeWeights(w){
      const keys = Object.keys(w);
      const sum = keys.reduce((a,k)=> a + (Number(w[k]) || 0), 0);
      if (sum <= 0) {
        const u = 1 / keys.length;
        keys.forEach(k => w[k] = u);
        return w;
      }
      keys.forEach(k => w[k] = (Number(w[k]) || 0) / sum);
      return w;
    }

    function scoreRow(r, w){
      // components are 0..1 goodness
      const cost = r.cost_good ?? 0.5;
      const crime = r.crime_good ?? 0.5;
      const tax = r.tax_good ?? 0.5;
      const unemp = r.unemp_good ?? 0.5;
      const employ = r.employ_good ?? 0.5;
      const wage = r.wage_good ?? 0.5;

      return (
        w.w_cost * cost +
        w.w_safety * crime +
        w.w_tax * tax +
        w.w_unemp * unemp +
        w.w_employ * employ +
        w.w_wage * wage
      );
    }

    function recomputeAndSort(){
      const w = normalizeWeights({ ...W });
      // persist
      localStorage.setItem("livability_weights", JSON.stringify(w));

      // recompute
      rows = data.map(r => {
        const s = scoreRow(r, w);
        return { ...r, livability_score: s };
      });

      // sort + rank
      rows.sort((a,b) => (b.livability_score - a.livability_score) || a.state.localeCompare(b.state));
      rows = rows.map((r, i) => ({ ...r, rank: i + 1 }));

      // refresh UI
      const q = (search.value || "").toLowerCase();
      const filtered = q ? rows.filter(r => r.state.toLowerCase().includes(q)) : rows;

      fillTable(filtered);
      fillSelect(filtered);

      if (filtered.length) {
        // keep selected state if still present, else first
        const keep = filtered.find(x => x.state === select.value);
        select.value = keep ? keep.state : filtered[0].state;
        renderState(select.value, rows);
      }

      renderWeightsUI(w);
    }

    function fillTable(list){
      tbody.innerHTML = "";
      list.slice(0,25).forEach(r => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.rank}</td>
          <td>${r.state}</td>
          <td class="score">${Number(r.livability_score).toFixed(3)}</td>
          <td>${r.Cost_of_Living_Index_2025 == null ? "—" : Number(r.Cost_of_Living_Index_2025).toFixed(1)}</td>
          <td>${r.Top_State_Income_Tax_Rate == null ? "—" : (Number(r.Top_State_Income_Tax_Rate).toFixed(2) + "%")}</td>
        `;
        tr.style.cursor = "pointer";
        tr.addEventListener("click", () => {
          select.value = r.state;
          renderState(r.state, rows);
        });
        tbody.appendChild(tr);
      });
    }

    function fillSelect(list){
      select.innerHTML = "";
      list.forEach(r => {
        const opt = document.createElement("option");
        opt.value = r.state;
        opt.textContent = r.state;
        select.appendChild(opt);
      });
    }

    function setKPIs(stateData){
      const el = document.getElementById("kpis");
      el.innerHTML = "";

      const cards = [
        {label:"Livability score", value: stateData.livability_score, sub:"0–1 scale", digits:3},
        {label:"Cost index (2025)", value: stateData.Cost_of_Living_Index_2025, sub:"Index value", digits:1},
        {label:"Top income tax", value: stateData.Top_State_Income_Tax_Rate, sub:"Percent", digits:2, suffix:"%"},
        {label:"Unemployment", value: stateData.unemployment_rate, sub:"Avg of recent months", digits:2, suffix:"%"},
        {label:"Employment", value: stateData.employment_rate, sub:"Avg of recent months", digits:2, suffix:"%"},
        {label:"Avg weekly earnings", value: stateData.avg_weekly_earnings, sub:"USD", digits:0},
        {label:"Violent crime rate", value: stateData.violent_crime_rate_per_100k, sub:"per 100k", digits:1},
      ];

      cards.forEach(c => {
        const div = document.createElement("div");
        div.className = "kpi";
        const v = (c.value === null || c.value === undefined) ? null : c.value;
        div.innerHTML = `
          <div class="lbl">${c.label}</div>
          <div class="val">${v === null ? "—" : (typeof v === "number" ? Number(v).toFixed(c.digits) : v)}${c.suffix ? (v===null?"":c.suffix) : ""}</div>
          <div class="sub">${c.sub}</div>
        `;
        el.appendChild(div);
      });
    }

    function renderState(stateName, allRows){
      const stateData = allRows.find(r => r.state === stateName);
      if (!stateData) return;
      setKPIs(stateData);
    }

    function renderWeightsUI(w){
      weightsPanel.innerHTML = `
        <div class="weights-title">
          Drag the sliders to set your priorities.
          <b>Higher weight = you care more about that factor.</b>
          (Weights auto-normalize to 100%.)
        </div>
        <div class="range-hints">
          <span>Less important</span>
          <span>More important</span>
        </div>
      `;

      const defs = [
        {key:"w_cost", label:"Cost"},
        {key:"w_safety", label:"Safety"},
        {key:"w_tax", label:"Tax"},
        {key:"w_unemp", label:"Unemployment"},
        {key:"w_employ", label:"Employment"},
        {key:"w_wage", label:"Wage"},
      ];

      defs.forEach(d => {
        const row = document.createElement("div");
        row.className = "wrow";
        row.innerHTML = `
          <div class="wlabel">${d.label}</div>
          <input type="range" min="0" max="1" step="0.01" value="${w[d.key].toFixed(2)}" data-k="${d.key}">
          <div class="wval" id="val_${d.key}">${w[d.key].toFixed(2)}</div>
        `;
        weightsPanel.appendChild(row);
      });

      // bind listeners
      weightsPanel.querySelectorAll('input[type="range"]').forEach(inp => {
        inp.addEventListener("input", (e) => {
          const k = e.target.getAttribute("data-k");
          W[k] = Number(e.target.value);
          recomputeAndSort();
        });
      });
    }

    // search/filter
    search.addEventListener("input", () => recomputeAndSort());
    select.addEventListener("change", () => renderState(select.value, rows));

    // initial render
    recomputeAndSort();
  });
</script>
</body>
</html>
"""

    with open(DASHBOARD_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[dashboard] Wrote: {DASHBOARD_HTML_PATH}")
    print(f"[dashboard] Data:  {DASHBOARD_JSON_PATH}")


# ---------------------------------------------------------------------
# Optional: serve dashboard
# ---------------------------------------------------------------------

def serve_dashboard(port: int = 8000, open_browser: bool = True) -> None:
    url = f"http://localhost:{port}/data/dashboard.html"

    while True:
        try:
            httpd = ThreadingHTTPServer(("0.0.0.0", port), SimpleHTTPRequestHandler)
            break
        except OSError as e:
            if getattr(e, "errno", None) in (48, 98):
                port += 1
                url = f"http://localhost:{port}/data/dashboard.html"
                continue
            raise

    print("\n" + "=" * 60)
    print(f"[serve] Serving current folder on http://localhost:{port}/")
    print(f"Open: {url}")
    print("=" * 60)

    if open_browser:
        def _open():
            time.sleep(0.8)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[serve] Stopped.")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ONE entry point: pipeline + analysis + dashboard."
    )

    # Pipeline args
    parser.add_argument("--start", default="2022", help="BLS start year, e.g. 2022")
    parser.add_argument("--end", default="2025", help="BLS end year, e.g. 2025")
    parser.add_argument("--key", default=None, help="BLS API key (optional)")
    parser.add_argument(
        "--use-existing",
        action="store_true",
        help="Skip live scraping/API and use existing CSVs under data/ (recommended for stable demo).",
    )
    parser.add_argument(
    "--auto",
    action="store_true",
    help="One-click demo: build dashboard + serve + open browser automatically.",
)

    # Analysis args
    parser.add_argument("--months-back", type=int, default=12, help="How many recent months to average for the score.")
    parser.add_argument("--w_cost", type=float, default=0.20, help="Weight: affordability (Cost of Living). Higher means lower cost is rewarded.")
    parser.add_argument("--w_safety", type=float, default=0.18, help="Weight: safety (violent crime). Higher means lower crime is rewarded.")
    parser.add_argument("--w_tax", type=float, default=0.10, help="Weight: tax (top income tax). Higher means lower tax is rewarded.")
    parser.add_argument("--w_unemp", type=float, default=0.18, help="Weight: unemployment (lower is better).")
    parser.add_argument("--w_employ", type=float, default=0.12, help="Weight: employment rate (higher is better).")
    parser.add_argument("--w_wage", type=float, default=0.22, help="Weight: earnings (higher is better).")

    # Dashboard args
    parser.add_argument("--build-dashboard", action="store_true", help="Generate data/dashboard.html + data/dashboard_data.json")
    parser.add_argument("--serve", action="store_true", help="Serve the dashboard locally (after building).")
    parser.add_argument("--port", type=int, default=8000, help="Port for --serve (default 8000).")

    args = parser.parse_args()
    if args.auto:
      args.build_dashboard = True
      args.serve = True

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1–3: pipeline (unless using cached CSVs)
    if not args.use_existing:
        run_bls(args.start, args.end, args.key)
        run_data_collecting()
        merge_and_export()
    else:
        print("\n" + "=" * 60)
        print("[skip] Using existing CSVs in data/ (no live scraping/API).")
        print("=" * 60)

    # Validate required inputs for analysis/dashboard
    if not MONTHLY_MERGED_PATH.exists():
        raise FileNotFoundError(
            f"Missing {MONTHLY_MERGED_PATH}. Run pipeline once or place a cached copy there."
        )
    if not COST_TAX_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Missing {COST_TAX_CSV_PATH}. Run data_collecting once or place a cached copy there."
        )

    # Step 4: analysis
    print("\n" + "=" * 60)
    print("[step 4] Analysis: compute livability ranking")
    print("=" * 60)

    weights = {
        "w_cost": args.w_cost,
        "w_safety": args.w_safety,
        "w_tax": args.w_tax,
        "w_unemp": args.w_unemp,
        "w_employ": args.w_employ,
        "w_wage": args.w_wage,
    }
    rankings_df = compute_livability_rankings(
        monthly_merged_path=MONTHLY_MERGED_PATH,
        cost_tax_path=COST_TAX_CSV_PATH,
        months_back=args.months_back,
        weights=weights,
    )
    rankings_df.to_csv(RANKINGS_CSV_PATH, index=False)
    print(f"[step 4] -> {RANKINGS_CSV_PATH}  ({len(rankings_df):,} rows)")
    print(rankings_df.head(10).to_string(index=False))

    # Step 5: dashboard
    if args.build_dashboard:
        print("\n" + "=" * 60)
        print("[step 5] Build dashboard")
        print("=" * 60)
        build_dashboard(rankings_df)

    print("\n" + "=" * 60)
    print("[done] Outputs:")
    print(f"  - {MONTHLY_MERGED_PATH}")
    print(f"  - {COST_TAX_CSV_PATH}")
    print(f"  - {RANKINGS_CSV_PATH}")
    if args.build_dashboard:
        print(f"  - {DASHBOARD_HTML_PATH}")
        print(f"  - {DASHBOARD_JSON_PATH}")
    print("=" * 60)

    if args.serve and args.build_dashboard:
        serve_dashboard(port=args.port, open_browser=True)
    elif args.serve and not args.build_dashboard:
        print("[serve] Please add --build-dashboard first (to generate the HTML/JSON).")


if __name__ == "__main__":
    main()
