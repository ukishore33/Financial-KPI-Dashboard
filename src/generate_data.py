"""
generate_data.py
----------------
Generates synthetic 3-year (2022-2024) monthly financial data for
FinSight Capital — a fictional Indian NBFC.

Tables produced (saved as CSVs in data/):
  - loan_portfolio.csv
  - collections.csv
  - profit_loss.csv
  - risk_metrics.csv

Author : Kishore U. | github.com/ukishore33
"""

import os
import random
import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

# ── output directory ─────────────────────────────────────────────────────────
os.makedirs("data", exist_ok=True)

# ── constants ────────────────────────────────────────────────────────────────
MONTHS      = list(range(1, 13))
YEARS       = [2022, 2023, 2024]
PRODUCTS    = ["Personal", "Business", "Home", "Vehicle"]
SEGMENTS    = ["Retail", "MSME", "Corporate"]
GEOS        = ["Metro", "Tier1", "Tier2", "Rural"]
STATUSES    = ["Active", "Closed", "NPA", "Written-off"]
DPD_BUCKETS = [0, 30, 60, 90, 180, 360]

# DPD bucket weights per year — NPA rises 2022→2023, improves 2024
DPD_WEIGHTS = {
    2022: [0.72, 0.10, 0.07, 0.05, 0.04, 0.02],
    2023: [0.62, 0.12, 0.09, 0.08, 0.06, 0.03],
    2024: [0.70, 0.11, 0.07, 0.06, 0.04, 0.02],
}

# Status weights per year
STATUS_WEIGHTS = {
    2022: [0.68, 0.20, 0.09, 0.03],
    2023: [0.62, 0.20, 0.13, 0.05],
    2024: [0.67, 0.21, 0.09, 0.03],
}

INTEREST_RATES = {
    "Personal":  (13.5, 18.5),
    "Business":  (11.0, 16.0),
    "Home":      ( 8.5, 12.5),
    "Vehicle":   ( 9.5, 14.0),
}

DISBURSAL_BASE = {          # ₹ Crores base per month per product
    "Personal":  120,
    "Business":  180,
    "Home":      250,
    "Vehicle":   90,
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. loan_portfolio
# ─────────────────────────────────────────────────────────────────────────────
def generate_loan_portfolio() -> pd.DataFrame:
    records = []
    loan_id = 1000

    for year in YEARS:
        growth = 1 + (year - 2022) * 0.18          # 18 % YoY growth
        for month in MONTHS:
            season = 1 + 0.08 * np.sin(2 * np.pi * month / 12)  # seasonality

            for product in PRODUCTS:
                n_loans = int(random.randint(60, 90) * growth * season)
                base_dis = DISBURSAL_BASE[product] * growth * season

                for _ in range(n_loans):
                    loan_id += 1
                    product_type     = product
                    disbursed_amount = round(
                        base_dis * random.uniform(0.5, 1.8) / n_loans * 100, 2
                    )  # ₹ Lakhs
                    interest_rate    = round(
                        random.uniform(*INTEREST_RATES[product]), 2
                    )
                    status = random.choices(
                        STATUSES, weights=STATUS_WEIGHTS[year]
                    )[0]
                    dpd_bucket = random.choices(
                        DPD_BUCKETS, weights=DPD_WEIGHTS[year]
                    )[0]

                    # Outstanding ~ 60-95 % of disbursed for active loans
                    if status == "Active":
                        outstanding = round(disbursed_amount * random.uniform(0.60, 0.95), 2)
                    elif status == "Closed":
                        outstanding = 0.0
                    elif status == "NPA":
                        outstanding = round(disbursed_amount * random.uniform(0.70, 1.00), 2)
                    else:  # Written-off
                        outstanding = 0.0

                    emi = round(outstanding * (interest_rate / 1200) /
                                (1 - (1 + interest_rate / 1200) ** -36), 2) if outstanding else 0.0

                    records.append({
                        "loan_id"          : loan_id,
                        "month"            : month,
                        "year"             : year,
                        "product_type"     : product_type,
                        "disbursed_amount" : disbursed_amount,
                        "outstanding_amount": outstanding,
                        "status"           : status,
                        "customer_segment" : random.choice(SEGMENTS),
                        "geography"        : random.choices(
                            GEOS, weights=[0.35, 0.30, 0.22, 0.13]
                        )[0],
                        "interest_rate"    : interest_rate,
                        "emi_amount"       : emi,
                        "dpd_bucket"       : dpd_bucket,
                    })

    df = pd.DataFrame(records)
    df.to_csv("data/loan_portfolio.csv", index=False)
    print(f"[✓] loan_portfolio.csv — {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. collections
# ─────────────────────────────────────────────────────────────────────────────
def generate_collections(df_loans: pd.DataFrame) -> pd.DataFrame:
    """One row per active/NPA loan-month."""
    records = []
    active_loans = df_loans[df_loans["status"].isin(["Active", "NPA"])].copy()

    # Collection efficiency by year — dips 2023, recovers 2024
    CE_MEAN = {2022: 0.938, 2023: 0.905, 2024: 0.942}
    CE_STD  = 0.04

    for _, row in active_loans.iterrows():
        amount_due   = row["emi_amount"] if row["emi_amount"] > 0 else round(
            row["outstanding_amount"] * 0.035, 2
        )
        ce = min(1.0, max(0.0, np.random.normal(CE_MEAN[row["year"]], CE_STD)))
        # NPA loans have lower CE
        if row["status"] == "NPA":
            ce = min(ce, random.uniform(0.40, 0.70))
        amount_collected = round(amount_due * ce, 2)

        records.append({
            "loan_id"             : row["loan_id"],
            "month"               : row["month"],
            "year"                : row["year"],
            "amount_due"          : round(amount_due, 2),
            "amount_collected"    : amount_collected,
            "collection_efficiency": round(ce, 4),
        })

    df = pd.DataFrame(records)
    df.to_csv("data/collections.csv", index=False)
    print(f"[✓] collections.csv    — {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. profit_loss
# ─────────────────────────────────────────────────────────────────────────────
def generate_profit_loss() -> pd.DataFrame:
    records = []
    # Base NII in ₹ Crores; grows ~20 % YoY
    NII_BASE = {2022: 3.5, 2023: 4.2, 2024: 5.0}

    for year in YEARS:
        for month in MONTHS:
            nii          = round(NII_BASE[year] * random.uniform(0.92, 1.08), 2)
            opex         = round(nii * random.uniform(0.38, 0.46), 2)      # cost-to-income 38-46%
            # Provisions higher in 2023 (NPA spike)
            prov_ratio   = {2022: 0.12, 2023: 0.19, 2024: 0.13}[year]
            provisions   = round(nii * prov_ratio * random.uniform(0.9, 1.1), 2)
            pbt          = nii - opex - provisions
            tax          = round(max(0, pbt * 0.25), 2)
            pat          = round(pbt - tax, 2)
            net_margin   = round(pat / nii * 100, 2) if nii else 0

            records.append({
                "month"              : month,
                "year"               : year,
                "net_interest_income": nii,
                "opex"               : opex,
                "provisions"         : provisions,
                "pat"                : pat,
                "net_margin"         : net_margin,
            })

    df = pd.DataFrame(records)
    df.to_csv("data/profit_loss.csv", index=False)
    print(f"[✓] profit_loss.csv    — {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 4. risk_metrics
# ─────────────────────────────────────────────────────────────────────────────
def generate_risk_metrics() -> pd.DataFrame:
    records = []

    # GNPA trend: rises through 2023, improves 2024
    GNPA_TREND = {
        (2022, 1): 3.2,  (2022, 6): 3.8,  (2022, 12): 4.5,
        (2023, 1): 4.7,  (2023, 6): 5.8,  (2023, 12): 6.2,
        (2024, 1): 5.9,  (2024, 6): 5.1,  (2024, 12): 4.4,
    }

    def interp_gnpa(year, month):
        """Linear interpolation of GNPA across anchor points."""
        flat = {(y, m): v for (y, m), v in GNPA_TREND.items()}
        pts  = sorted(flat.keys())
        key  = (year, month)
        if key in flat:
            return flat[key]
        # find nearest bracketing points
        before = [p for p in pts if p <= key]
        after  = [p for p in pts if p > key]
        if not before:
            return flat[pts[0]]
        if not after:
            return flat[pts[-1]]
        k0, k1 = before[-1], after[0]
        t0 = k0[0] * 12 + k0[1]
        t1 = k1[0] * 12 + k1[1]
        t  = year  * 12 + month
        frac = (t - t0) / (t1 - t0)
        return flat[k0] + frac * (flat[k1] - flat[k0])

    for year in YEARS:
        for month in MONTHS:
            gnpa = round(interp_gnpa(year, month) + random.uniform(-0.15, 0.15), 2)
            nnpa = round(gnpa * random.uniform(0.48, 0.55), 2)   # typically ~50% of GNPA
            pcr  = round(random.uniform(58, 70), 2)               # PCR 58-70%
            car  = round(random.uniform(16.5, 21.0), 2)           # CAR well above 15% RBI norm
            cof  = round(random.uniform(7.2, 9.1), 2)             # Cost of Funds

            records.append({
                "month"       : month,
                "year"        : year,
                "gnpa_pct"    : gnpa,
                "nnpa_pct"    : nnpa,
                "pcr_pct"     : pcr,
                "car_pct"     : car,
                "cost_of_funds": cof,
            })

    df = pd.DataFrame(records)
    df.to_csv("data/risk_metrics.csv", index=False)
    print(f"[✓] risk_metrics.csv   — {len(df):,} rows")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n── Generating FinSight Capital synthetic data ──\n")
    df_loans = generate_loan_portfolio()
    generate_collections(df_loans)
    generate_profit_loss()
    generate_risk_metrics()
    print("\n[✓] All CSVs saved to data/\n")
