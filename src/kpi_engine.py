"""
kpi_engine.py
-------------
Orchestration script for the FinSight Capital KPI Dashboard.

Pipeline
  1. Load CSVs from data/ into an in-memory SQLite database
  2. Run all 9 SQL queries (defined in sql_queries.py)
  3. Compute derived KPI summaries (Python layer)
  4. Export results as kpis.json (consumed by the HTML dashboard)

Run: python src/kpi_engine.py

Author : Kishore U. | github.com/ukishore33
"""

import json
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# ── ensure src/ is importable ─────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from sql_queries import QUERIES

DATA_DIR   = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent.parent
OUTPUT_JSON = OUTPUT_DIR / "kpis.json"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Database loader
# ─────────────────────────────────────────────────────────────────────────────
def build_database() -> sqlite3.Connection:
    """Load all CSV tables into an in-memory SQLite database."""
    con = sqlite3.connect(":memory:")

    tables = {
        "loan_portfolio": "loan_portfolio.csv",
        "collections"   : "collections.csv",
        "profit_loss"   : "profit_loss.csv",
        "risk_metrics"  : "risk_metrics.csv",
    }

    for table_name, filename in tables.items():
        csv_path = DATA_DIR / filename
        if not csv_path.exists():
            raise FileNotFoundError(
                f"[✗] {csv_path} not found. "
                "Run `python src/generate_data.py` first."
            )
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"  [DB] Loaded {table_name:<20} — {len(df):>7,} rows")

    return con


# ─────────────────────────────────────────────────────────────────────────────
# 2. Query runner
# ─────────────────────────────────────────────────────────────────────────────
def run_queries(con: sqlite3.Connection) -> dict[str, list[dict]]:
    """Execute all SQL queries, return results as list-of-dicts."""
    results = {}
    for name, sql in QUERIES.items():
        try:
            df = pd.read_sql_query(sql, con)
            results[name] = df.to_dict(orient="records")
            print(f"  [Q]  {name:<28} — {len(df):>5} rows")
        except Exception as e:
            print(f"  [!]  {name:<28} — ERROR: {e}")
            results[name] = []
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 3. Python-layer KPI summaries
# ─────────────────────────────────────────────────────────────────────────────
def compute_summaries(raw: dict[str, list[dict]]) -> dict:
    """Derive headline KPI cards and YTD/3-year summary stats."""

    def latest(series: list[dict], key: str):
        """Return the last non-null value for a key in a time series."""
        vals = [r[key] for r in series if r.get(key) is not None]
        return vals[-1] if vals else None

    def avg(series: list[dict], key: str):
        vals = [r[key] for r in series if r.get(key) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    aum      = raw["aum_trend"]
    npa      = raw["npa_trend"]
    col      = raw["collection_efficiency"]
    nim      = raw["nim"]
    cti      = raw["cost_to_income"]
    par      = raw["par"]
    dis      = raw["disbursement_growth"]
    pcr      = raw["pcr"]
    geo_npa  = raw["top5_geo_npa"]

    # latest period values for KPI cards
    cards = {
        "aum_latest_crores"     : latest(aum,  "aum_crores"),
        "gnpa_latest_pct"       : latest(npa,  "gnpa_pct"),
        "nnpa_latest_pct"       : latest(npa,  "nnpa_pct"),
        "collection_eff_avg_pct": avg(col, "collection_efficiency_pct"),
        "nim_annualised_pct"    : latest(nim,  "nim_annualised_pct"),
        "cti_latest_pct"        : latest(cti,  "cost_to_income_pct"),
        "par30_latest_pct"      : latest(par,  "par_30_pct"),
        "par60_latest_pct"      : latest(par,  "par_60_pct"),
        "par90_latest_pct"      : latest(par,  "par_90_pct"),
        "yoy_disbursement_pct"  : latest(dis,  "yoy_growth_pct"),
        "pcr_latest_pct"        : latest(pcr,  "pcr_from_metrics"),
    }

    # Collection efficiency by product (2024 only, aggregated)
    col_by_product = {}
    for row in col:
        if row.get("year") == 2024:
            p = row["product_type"]
            col_by_product.setdefault(p, []).append(row["collection_efficiency_pct"])
    col_by_product_avg = {p: round(sum(v)/len(v), 2) for p, v in col_by_product.items()}

    # Collection efficiency by geography (2024 only, aggregated)
    col_by_geo = {}
    for row in col:
        if row.get("year") == 2024:
            g = row["geography"]
            col_by_geo.setdefault(g, []).append(row["collection_efficiency_pct"])
    col_by_geo_avg = {g: round(sum(v)/len(v), 2) for g, v in col_by_geo.items()}

    # PAR bucket absolute values for latest month
    par_buckets_latest = {}
    if par:
        last = par[-1]
        for bucket in ["dpd_0_cr", "dpd_30_cr", "dpd_60_cr",
                       "dpd_90_cr", "dpd_180_cr", "dpd_360_cr"]:
            par_buckets_latest[bucket] = last.get(bucket)

    return {
        "kpi_cards"              : cards,
        "col_by_product_2024"    : col_by_product_avg,
        "col_by_geo_2024"        : col_by_geo_avg,
        "par_buckets_latest"     : par_buckets_latest,
        "top5_geo_npa"           : geo_npa,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Export to JSON
# ─────────────────────────────────────────────────────────────────────────────
def export_json(raw_results: dict, summaries: dict):
    output = {
        "meta": {
            "company"    : "FinSight Capital",
            "generated"  : pd.Timestamp.now().isoformat(),
            "data_range" : "Jan 2022 – Dec 2024",
            "currency"   : "INR Crores",
        },
        "summaries" : summaries,
        "timeseries": raw_results,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n[✓] kpis.json exported → {OUTPUT_JSON}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("\n══════════════════════════════════════════════════")
    print("  FinSight Capital — KPI Engine")
    print("══════════════════════════════════════════════════\n")

    print("[Step 1] Loading data into SQLite …")
    con = build_database()

    print("\n[Step 2] Running SQL queries …")
    raw = run_queries(con)

    print("\n[Step 3] Computing Python-layer summaries …")
    summaries = compute_summaries(raw)
    for k, v in summaries["kpi_cards"].items():
        print(f"  {k:<35} : {v}")

    print("\n[Step 4] Exporting JSON …")
    export_json(raw, summaries)

    con.close()
    print("\n[✓] Done — open financial_kpi_dashboard.html in your browser.\n")


if __name__ == "__main__":
    main()
