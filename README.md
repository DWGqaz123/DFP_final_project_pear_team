## Team Information

*Team Name:* Pear Team 

*Group Members:*
•⁠  ⁠Rachel Chen (rachelc3)  
•⁠  ⁠Chih-Yu Liu (chihyul3)  
•⁠  ⁠Wenguang Dong (wenguand)  
•⁠  ⁠Susana Peng (susanap)  

---

# BLS State Labor Market Data Pipeline

A collection of Python scripts that fetch, merge, and export US state-level labor market data from the BLS Public Data API v2.

---

## Project Structure
```
├── dashboard.html                   # Visual dashboard frontend (static HTML UI)
│
├── data/                            # Data storage and dashboard assets
│   ├── bls_data/                    # Raw and processed BLS-specific CSV outputs
│   │   ├── bls_avg_weekly_hours_50states.csv
│   │   ├── bls_avg_weekly_wage_50states.csv
│   │   ├── bls_employment_rate_50states.csv
│   │   ├── bls_job_opennings_rate_50states.csv
│   │   ├── bls_master.csv           # Master merged BLS dataset
│   │   ├── bls_quits_level_50states.csv
│   │   └── bls_unemployment_rate_50states.csv
│   │
│   ├── crime_rate_2022_2025_monthly.csv   # Scraped violent crime rate data
│   ├── dashboard_data.json                # Data formatted for the web dashboard
│   ├── monthly_merged.csv                 # Final merged BLS + supplemental data
│   └── state_cost_tax_2025.csv            # Scraped cost of living + income tax data
│
├── main_run.py                     # Top-level pipeline: BLS + supplemental data → final CSVs + dashboard JSON
├── requirements.txt                # Python dependencies
│
└── scrapers/                       # Data collection scripts
    ├── bls_avg_weekly_hours_50states.py
    ├── bls_avg_weekly_wage_50states.py
    ├── bls_employment_rate_50states.py
    ├── bls_job_opennings_rate_50states.py
    ├── bls_quits_level_50states.py
    ├── bls_run.py                  # BLS master runner: initializes and runs all BLS sub-scripts
    ├── bls_unemployment_rate_50states.py
    └── data_collecting.py          # Scrapes cost of living, income tax, and crime rate data
```

---

## Data Sources

| Dataset | Source | Series Type |
|---|---|---|
| Unemployment Rate | BLS LAUS | `LAUST{FIPS}0000000000003` |
| Employment Rate | BLS LAUS | `LAUST{FIPS}0000000000007` |
| Avg Weekly Hours | BLS SMU | `SMU{FIPS}000000500000002` |
| Job Openings Rate | BLS JOLTS | `JTS000000{FIPS}0000000JOR` |
| Quits Level | BLS JOLTS | `JTU000000{FIPS}0000000QUL` |
| Avg Weekly Earnings | BLS SMU | `SMU{FIPS}000000500000011` |
| Cost of Living Index | worldpopulationreview.com | Scraped |
| Top Income Tax Rate | Wikipedia | Scraped |
| Violent Crime Rate | FBI CDE API | REST API |

---

## Output Columns (bls_master.csv / final_merged.csv)

| Column | Description | Unit |
|---|---|---|
| `state` | Full state name | — |
| `state_abbr` | Two-letter abbreviation | — |
| `fips` | State FIPS code | — |
| `year` | Year | — |
| `month` | Month number | 1–12 |
| `date` | First day of month | YYYY-MM-DD |
| `unemployment_rate` | Unemployment rate | % |
| `employment_rate` | Employment rate | % |
| `avg_weekly_hours` | Avg weekly hours worked | Hours |
| `job_openings_rate` | Job openings rate | % |
| `quits_level_thousands` | Number of quits | Thousands |
| `avg_weekly_earnings` | Avg weekly earnings | USD |
| `violent_crime_rate_per_100k` | Violent crime rate | Per 100k people |

---

## Usage

### Init the environment 
```bash
pip install -r requirements.txt
```

### Full pipeline (recommended)
```bash
Step 1:
cd web_scraping

python3 main_run.py --auto --start 2022 --end 2025 --key 963729bfa50042e294f9e0516067fcb7
or
python main_run.py --auto --start 2022 --end 2025 --key 963729bfa50042e294f9e0516067fcb7

Step 2:
cd web_scraper
python3 -m http.server 8000

Step 3: 
open: http://localhost:8000/dashboard.html

```

---

## Notes

- BLS API allows a maximum range of **20 years** per request
- Without an API key, batch size is reduced from 50 to 25 series per request
- A free BLS API key can be registered at: https://data.bls.gov/registrationEngine/
- The FBI Crime API (`api.usa.gov`) may be intermittently unavailable; the pipeline will skip crime data gracefully if the API is unreachable
- All output files are written to the `data/` directory