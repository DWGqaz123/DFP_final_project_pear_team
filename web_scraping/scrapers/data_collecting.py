"""
State Data Collection Pipeline
--------------------------------
This script collects:
1. Cost of Living Index (2025)
2. Top State Income Tax Rate (Single Filer)
3. Monthly Violent Crime Rates (2022–2025)

Outputs:
- state_cost_tax_2025.csv
- crime_rate_2022_2025_monthly.csv
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import time
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import FBI_API_KEY



# 1 Helper Dictionaries

STATE_ABBREV = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR",
    "California":"CA","Colorado":"CO","Connecticut":"CT","Delaware":"DE",
    "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID",
    "Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS",
    "Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
    "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
    "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
    "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK",
    "Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC",
    "South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT",
    "Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
    "Wisconsin":"WI","Wyoming":"WY","District of Columbia":"DC"
}

STATE_FULL_NAMES = {v: k for k, v in STATE_ABBREV.items()}


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# 2 Scrape Cost of Living Index

def scrape_cost_of_living():
    print("Scraping Cost of Living Index...")

    url = "https://worldpopulationreview.com/state-rankings/cost-of-living-index-by-state"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    scripts = soup.find_all("script")
    data_lookup_text = None

    for script in scripts:
        if script.string and "const dataLookup" in script.string:
            data_lookup_text = script.string
            break

    match = re.search(r'const dataLookup\s*=\s*(\{.*?\});', data_lookup_text, re.S) # pyright: ignore[reportArgumentType, reportCallIssue]
    json_text = match.group(1)
    data_dict = json.loads(json_text)

    df_cost = pd.DataFrame(
        list(data_dict.items()),
        columns=["State", "Cost_of_Living_Index_2025"]
    )

    df_cost["Cost_of_Living_Index_2025"] = df_cost["Cost_of_Living_Index_2025"].astype(float)

    return df_cost


# 3 Scrape Top State Income Tax Rate

def scrape_income_tax():
    print("Scraping Top State Income Tax Rates...")

    url = "https://en.wikipedia.org/wiki/State_income_tax"
    
    # add a header to mimic a real browser
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    
    response = requests.get(url, headers=headers, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        raise RuntimeError(
            "No wikitable found on Wikipedia State income tax page. "
            f"HTTP status: {response.status_code}. "
            "The page structure may have changed or the request was blocked."
        )

    target_table = tables[0]
    rows = target_table.find_all("tr")

    data = []
    current_state = None

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) >= 2:
            state_text = cols[0].get_text(strip=True)
            single_text = cols[1].get_text(strip=True)

            if state_text != "":
                current_state = state_text

            if single_text == "None":
                data.append([current_state, 0.0])
            else:
                match = re.search(r'(\d+\.?\d*)%', single_text)
                if match:
                    rate = float(match.group(1))
                    data.append([current_state, rate])

    df_tax = pd.DataFrame(data, columns=["State", "Rate"])
    df_max = df_tax.groupby("State")["Rate"].max().reset_index()
    df_max.columns = ["State", "Top_State_Income_Tax_Rate"]

    return df_max


# 4 Scrape Monthly Violent Crime Rates

def scrape_crime_rates(api_key):
    print("Scraping Monthly Crime Rates...")

    states = list(STATE_FULL_NAMES.keys())
    all_data = []

    for state in states:
        url = f"https://api.usa.gov/crime/fbi/cde/summarized/state/{state}/V"

        params = {
            "from": "01-2022",
            "to": "12-2025",
            "API_KEY": api_key
        }

        response = requests.get(url, params=params, timeout=15)

        if response.status_code == 200:
            data = response.json()
            rates = data["offenses"]["rates"]

            state_rates = None

            for key in rates:
                if "Offenses" in key and "United States" not in key:
                    state_rates = rates[key]
                    break

            if state_rates:
                for month, value in state_rates.items():
                    if value is not None:
                        all_data.append({
                            "State": STATE_FULL_NAMES[state],
                            "State_Abbreviation": state,
                            "Month": month,
                            "Violent_Crime_Rate_per_100k": value
                        })

        time.sleep(0.3)

    df_crime = pd.DataFrame(all_data)
    return df_crime


# 5 Main Execution

def main():
    api_key = FBI_API_KEY

    # Collect datasets
    df_cost = scrape_cost_of_living()
    df_tax = scrape_income_tax()
    if api_key:
        df_crime = scrape_crime_rates(api_key)
    else:
        print("FBI_API_KEY is empty in config.py; skipping crime data scrape.")
        df_crime = pd.DataFrame(
            columns=[
                "State",
                "State_Abbreviation",
                "Month",
                "Violent_Crime_Rate_per_100k",
            ]
        )

    # Merge cost and tax
    df_merged = df_cost.merge(df_tax, on="State", how="left")
    df_merged["State_Abbreviation"] = df_merged["State"].map(STATE_ABBREV)

    df_merged = df_merged[
        [
            "State",
            "State_Abbreviation",
            "Cost_of_Living_Index_2025",
            "Top_State_Income_Tax_Rate"
        ]
    ]

    # Export CSV files
    df_merged.to_csv("state_cost_tax_2025.csv", index=False)
    df_crime.to_csv("crime_rate_2022_2025_monthly.csv", index=False)

    print("All data successfully exported.")


if __name__ == "__main__":
    main()
