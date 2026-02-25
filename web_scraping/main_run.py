#!/usr/bin/env python3
"""
main_run.py — ONE entry point for the whole project

Updates:
- Fixed IntCastingNaNError with fillna(0)
- Fixed Integer display for Current Rank (#1 instead of #1.0)
- Styled Pear Team Branding (Gray/Brown)
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
from typing import Dict
import pandas as pd
import numpy as np

# --- Paths / config ---
DATA_DIR = Path("data")
BLS_DIR = DATA_DIR / "bls_data"
BLS_MASTER_PATH = BLS_DIR / "bls_master.csv"
CRIME_CSV_PATH = DATA_DIR / "crime_rate_2022_2025_monthly.csv"
COST_TAX_CSV_PATH = DATA_DIR / "state_cost_tax_2025.csv"
MONTHLY_MERGED_PATH = DATA_DIR / "monthly_merged.csv"
RANKINGS_CSV_PATH = DATA_DIR / "livability_rankings.csv"
DASHBOARD_JSON_PATH = DATA_DIR / "dashboard_data.json"
DASHBOARD_HTML_PATH = DATA_DIR / "dashboard.html"

def run_bls(start: str, end: str, api_key: str | None) -> None:
    print("\n" + "=" * 60 + "\n[step 1] Running BLS pipeline\n" + "=" * 60)
    BLS_DIR.mkdir(parents=True, exist_ok=True)
    bls_script = Path("scrapers/bls_run.py")
    if not bls_script.exists():
        print(f"[ERROR] scrapers/bls_run.py not found"); sys.exit(1)
    cmd = [sys.executable, str(bls_script), "--start", start, "--end", end]
    if api_key: cmd += ["--key", api_key]
    subprocess.run(cmd)

def run_data_collecting() -> None:
    print("\n" + "=" * 60 + "\n[step 2] Running data collection\n" + "=" * 60)
    script = Path("scrapers/data_collecting.py")
    if not script.exists():
        print(f"[ERROR] scrapers/data_collecting.py not found"); sys.exit(1)
    subprocess.run([sys.executable, str(script)])
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for f in ["crime_rate_2022_2025_monthly.csv", "state_cost_tax_2025.csv"]:
        src = Path(f)
        if src.exists(): src.replace(DATA_DIR / f)

def parse_crime_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df["_date"] = pd.to_datetime(df["Month"], errors="coerce")
    df["year"] = df["_date"].dt.year
    df["month"] = df["_date"].dt.month
    df["state_abbr"] = df["State_Abbreviation"].astype(str).str.strip().str.upper()
    df["violent_crime_rate_per_100k"] = pd.to_numeric(df["Violent_Crime_Rate_per_100k"], errors="coerce")
    return df[["state_abbr", "year", "month", "violent_crime_rate_per_100k"]].dropna().astype({"year":int, "month":int})

def merge_and_export() -> None:
    print("\n" + "=" * 60 + "\n[step 3] Merging datasets\n" + "=" * 60)
    bls = pd.read_csv(BLS_MASTER_PATH, parse_dates=["date"])
    bls["state_abbr"] = bls["state_abbr"].astype(str).str.strip().str.upper()
    if CRIME_CSV_PATH.exists():
        crime = parse_crime_csv(CRIME_CSV_PATH)
        merged = bls.merge(crime, on=["state_abbr", "year", "month"], how="left")
    else: merged = bls.copy()
    merged.sort_values(["state", "year", "month"]).to_csv(MONTHLY_MERGED_PATH, index=False)

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(), s.max()
    return (s - mn) / (mx - mn) if mx != mn else pd.Series(0.5, index=s.index)

def compute_livability_rankings(monthly_merged_path: Path, cost_tax_path: Path, months_back: int, weights: Dict[str, float]) -> pd.DataFrame:
    m, ct = pd.read_csv(monthly_merged_path), pd.read_csv(cost_tax_path)
    m["date"] = pd.to_datetime(m["date"])
    recent = m[m["date"] >= (m["date"].max() - pd.DateOffset(months=months_back-1))].copy()
    agg_cols = ["unemployment_rate", "employment_rate", "avg_weekly_earnings", "violent_crime_rate_per_100k"]
    grp = recent.groupby("state", as_index=False)[agg_cols].mean()
    ct = ct.rename(columns={"State": "state"})
    grp = grp.merge(ct[["state", "Cost_of_Living_Index_2025", "Top_State_Income_Tax_Rate"]], on="state", how="left")
    
    comps = {
        "unemp_good": 1 - _minmax(grp["unemployment_rate"]),
        "crime_good": 1 - _minmax(grp["violent_crime_rate_per_100k"]),
        "cost_good": 1 - _minmax(grp["Cost_of_Living_Index_2025"]),
        "tax_good": 1 - _minmax(grp["Top_State_Income_Tax_Rate"]),
        "employ_good": _minmax(grp["employment_rate"]),
        "wage_good": _minmax(grp["avg_weekly_earnings"])
    }
    for k, v in comps.items(): grp[k] = v
    
    w_sum = sum(weights.values())
    nw = {k: v/w_sum for k, v in weights.items()}
    grp["livability_score"] = (nw['w_cost']*grp['cost_good'] + nw['w_safety']*grp['crime_good'] + 
                               nw['w_tax']*grp['tax_good'] + nw['w_unemp']*grp['unemp_good'] + 
                               nw['w_employ']*grp['employ_good'] + nw['w_wage']*grp['wage_good'])
    
    # FIX: Handle NaNs before casting to int
    grp["livability_score"] = grp["livability_score"].fillna(0)
    grp["rank"] = grp["livability_score"].rank(ascending=False, method="min").astype(int)
    return grp.sort_values("rank")

def build_dashboard(rankings_df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    clean_df = rankings_df.replace({np.nan: None})
    json.dump(clean_df.to_dict(orient="records"), open(DASHBOARD_JSON_PATH, "w"), indent=2)

    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Pear Team — State Livability Dashboard</title>
  <style>
    :root {
      --bg: #f6f2e8; --card: #ffffff; --text: #2f2416; --muted: #6b5a43;
      --accent: #c9a66b; --accent2: #a67c52;
      --line: rgba(47, 36, 22, 0.10); --radius: 16px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: -apple-system, sans-serif;
      background: radial-gradient(900px 380px at 20% 0%, rgba(201,166,107,0.25), transparent 60%), var(--bg);
      color: var(--text); padding: 32px;
    }
    .grid { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; align-items: start; }
    .card { background: var(--card); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: 0 10px 35px rgba(0,0,0,0.08); overflow: hidden; }
    .card .bd { position: relative; max-height: 720px; overflow-y: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { background: #c9a66b; color: #fff; position: sticky; top: 0; z-index: 20; padding: 10px; text-align: left; }
    td { padding: 10px; border-bottom: 1px solid rgba(0,0,0,0.05); }
    .kpis { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; padding: 15px; }
    .kpi { border: 1px solid var(--line); border-radius: 14px; padding: 12px; background: rgba(246,242,232,0.55); min-height: 85px; }
    .kpi .lbl { color: var(--muted); font-size: 11px; text-transform: uppercase; }
    .kpi .val { font-size: 18px; font-weight: 800; }
    input[type="number"] { padding: 8px; border-radius: 8px; border: 1px solid #ccc; width: 100%; margin-top: 4px; }
    #updateBtn { width: 100%; margin-top: 12px; padding: 10px; background: var(--accent2); color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; }
  </style>
</head>
<body>
  <div class="grid">
    <div class="card"><div class="hd" style="padding:15px;"><h2>Top 25 States</h2></div><div class="bd"><table><thead><tr><th>Rank</th><th>State</th><th>Score</th><th>Cost</th><th>Tax</th></tr></thead><tbody id="rankTableBody"></tbody></table></div></div>
    <div class="card"><div class="hd" style="padding:15px;"><h2>State Detail</h2></div><div class="bd">
      <div style="padding:15px;"><select id="stateSelect" style="width:100%; padding:10px; border-radius:8px;"></select></div>
      <div style="padding:15px; background:rgba(0,0,0,0.03); margin:15px; border-radius:12px;">
        <div style="font-weight:600; font-size:12px;">Importance Weights (0.0 - 1.0)</div>
        <div id="inputGrid" style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:10px;"></div>
        <button id="updateBtn">Update Rankings</button>
      </div>
      <div class="kpis" id="kpis"></div>
    </div></div>
  </div>
<script>
fetch("dashboard_data.json").then(r => r.json()).then(data => {
  let rows = data;
  const DEFAULT_W = { w_cost: 0.2, w_safety: 0.18, w_tax: 0.1, w_unemp: 0.18, w_employ: 0.12, w_wage: 0.22 };
  let W = JSON.parse(localStorage.getItem("livability_weights")) || DEFAULT_W;

  function recompute(){
    const sum = Object.values(W).reduce((a,b)=>a+b,0);
    const nw = {}; Object.keys(W).forEach(k => nw[k] = W[k]/(sum||1));
    rows = data.map(r => ({ ...r, livability_score: (nw.w_cost*(r.cost_good??0.5) + nw.w_safety*(r.crime_good??0.5) + nw.w_tax*(r.tax_good??0.5) + nw.w_unemp*(r.unemp_good??0.5) + nw.w_employ*(r.employ_good??0.5) + nw.w_wage*(r.wage_good??0.5)) }));
    rows.sort((a,b) => b.livability_score - a.livability_score);
    rows.forEach((r,i) => r.rank = i+1);
    fillTable(); renderState(document.getElementById("stateSelect").value || rows[0].state);
  }

  function fillTable(){
    const tb = document.getElementById("rankTableBody"); tb.innerHTML = "";
    rows.slice(0,25).forEach(r => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${r.rank}</td><td>${r.state}</td><td style="font-weight:700; color:#a67c52;">${r.livability_score.toFixed(3)}</td><td>${r.Cost_of_Living_Index_2025??'—'}</td><td>${r.Top_State_Income_Tax_Rate??'—'}%</td>`;
      tr.onclick = () => { document.getElementById("stateSelect").value = r.state; renderState(r.state); };
      tb.appendChild(tr);
    });
  }

  function renderState(name){
    const s = rows.find(r => r.state === name);
    const el = document.getElementById("kpis"); el.innerHTML = "";
    const cards = [
      // FIX: Math.floor used here for Integer Rank
      {l:"Current Rank", v: Math.floor(s.rank), p:"#", isRank:true}, 
      {l:"Score", v:s.livability_score, d:3}, 
      {l:"Cost Index", v:s.Cost_of_Living_Index_2025}, 
      {l:"Tax", v:s.Top_State_Income_Tax_Rate, s:"%"}, 
      {l:"Unemployment", v:s.unemployment_rate, s:"%"}, 
      {l:"Employment", v:s.employment_rate, s:"%"}, 
      {l:"Wage", v:s.avg_weekly_earnings}, 
      {l:"Crime", v:s.violent_crime_rate_per_100k}
    ];
    cards.forEach(c => {
      const d = document.createElement("div"); d.className = "kpi";
      if(c.isRank) d.style.background = "#ececec";
      d.innerHTML = `<div class="lbl">${c.l}</div><div class="val" style="color:${c.isRank?'#a67c52':'inherit'}">${c.p||""}${c.v != null ? (c.isRank ? c.v : c.v.toFixed(c.d??1)) : "—"}${c.s||""}</div>`;
      el.appendChild(d);
    });
  }

  const ig = document.getElementById("inputGrid");
  Object.keys(DEFAULT_W).forEach(k => {
    const div = document.createElement("div");
    div.innerHTML = `<label style="font-size:10px;">${k.split('_')[1].toUpperCase()}</label><input type="number" step="0.01" value="${W[k]}" id="in_${k}">`;
    ig.appendChild(div);
  });

  document.getElementById("updateBtn").onclick = () => {
    Object.keys(W).forEach(k => W[k] = parseFloat(document.getElementById("in_"+k).value)||0);
    localStorage.setItem("livability_weights", JSON.stringify(W));
    recompute();
  };

  const sel = document.getElementById("stateSelect");
  rows.forEach(r => { const o = document.createElement("option"); o.value = r.state; o.textContent = r.state; sel.appendChild(o); });
  sel.onchange = () => renderState(sel.value);
  recompute();
});
</script>
</body>
</html>"""
    with open(DASHBOARD_HTML_PATH, "w") as f: f.write(html_content)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ONE entry point: pipeline + analysis + dashboard."
    )

    # ADDING THESE BACK FIXES THE "UNRECOGNIZED ARGUMENTS" ERROR
    parser.add_argument("--start", default="2022", help="BLS start year")
    parser.add_argument("--end", default="2025", help="BLS end year")
    parser.add_argument("--key", default=None, help="BLS API key")
    parser.add_argument("--use-existing", action="store_true", help="Skip live scraping")
    parser.add_argument("--auto", action="store_true", help="Build + Serve + Open")

    args = parser.parse_args()

    if args.auto:
        # Step 1-3: Only run if not using existing data
        if not args.use_existing:
            run_bls(args.start, args.end, args.key)
            run_data_collecting()
            merge_and_export()
        
        # Step 4: Analysis (Using weights you defined)
        w = {"w_cost": 0.20, "w_safety": 0.18, "w_tax": 0.10, "w_unemp": 0.18, "w_employ": 0.12, "w_wage": 0.22}
        rankings = compute_livability_rankings(MONTHLY_MERGED_PATH, COST_TAX_CSV_PATH, 12, w)
        
        # Step 5: Build Dashboard
        build_dashboard(rankings)
        print(f"\n[DONE] Dashboard updated.")

if __name__ == "__main__":
    main()
