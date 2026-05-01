"""
sql_queries.py
--------------
Eight production-grade SQL queries (via SQLite) for FinSight Capital KPIs.

KPIs covered
  Q1  — AUM trend (monthly)
  Q2  — GNPA & NNPA ratio trend (monthly)
  Q3  — Collection efficiency by product & geography
  Q4  — Net Interest Margin (NIM) proxy (monthly)
  Q5  — Cost-to-Income ratio (monthly)
  Q6  — Portfolio at Risk (PAR 30 / 60 / 90) by DPD bucket
  Q7  — Disbursement growth (MoM % and YoY %)
  Q8  — Provision Coverage Ratio (PCR) trend
  Q9  — Top 5 geographies by NPA contribution (BONUS)

Author : Kishore U. | github.com/ukishore33
"""

# ── SQL string definitions ────────────────────────────────────────────────────

Q1_AUM_TREND = """
-- Q1: AUM (Assets Under Management) — monthly outstanding book
-- AUM = sum of outstanding_amount for Active + NPA loans (Written-off excluded)
SELECT
    lp.year,
    lp.month,
    ROUND(SUM(lp.outstanding_amount) / 100.0, 2)   AS aum_crores,
    lp.year || '-' || PRINTF('%02d', lp.month)      AS period
FROM loan_portfolio lp
WHERE lp.status IN ('Active', 'NPA')
GROUP BY lp.year, lp.month
ORDER BY lp.year, lp.month;
"""

Q2_NPA_TREND = """
-- Q2: Gross NPA % and Net NPA % — monthly
-- GNPA % = (Outstanding in NPA status) / (Total Outstanding Active + NPA) * 100
-- We cross-join with risk_metrics to also surface the pre-built GNPA/NNPA flags
SELECT
    rm.year,
    rm.month,
    rm.year || '-' || PRINTF('%02d', rm.month)   AS period,
    rm.gnpa_pct,
    rm.nnpa_pct,
    rm.pcr_pct,
    -- Portfolio-derived GNPA for reconciliation
    ROUND(
        100.0 * SUM(CASE WHEN lp.status = 'NPA' THEN lp.outstanding_amount ELSE 0 END)
        / NULLIF(SUM(CASE WHEN lp.status IN ('Active','NPA')
                         THEN lp.outstanding_amount END), 0),
        2
    )                                             AS portfolio_gnpa_pct
FROM risk_metrics rm
LEFT JOIN loan_portfolio lp
       ON lp.year = rm.year AND lp.month = rm.month
GROUP BY rm.year, rm.month
ORDER BY rm.year, rm.month;
"""

Q3_COLLECTION_EFFICIENCY = """
-- Q3: Collection efficiency by product_type and geography
-- Collection Efficiency = SUM(amount_collected) / SUM(amount_due) * 100
SELECT
    lp.year,
    lp.product_type,
    lp.geography,
    COUNT(DISTINCT c.loan_id)                                              AS loan_count,
    ROUND(SUM(c.amount_due), 2)                                            AS total_due,
    ROUND(SUM(c.amount_collected), 2)                                      AS total_collected,
    ROUND(100.0 * SUM(c.amount_collected) / NULLIF(SUM(c.amount_due),0), 2) AS collection_efficiency_pct
FROM collections c
JOIN loan_portfolio lp
  ON c.loan_id = lp.loan_id AND c.month = lp.month AND c.year = lp.year
GROUP BY lp.year, lp.product_type, lp.geography
ORDER BY lp.year, collection_efficiency_pct DESC;
"""

Q4_NIM = """
-- Q4: Net Interest Margin (NIM) — proxy using P&L data
-- NIM = Net Interest Income / Average AUM * 100  (annualised)
-- We approximate Average AUM from loan_portfolio outstanding
WITH monthly_aum AS (
    SELECT
        year, month,
        SUM(outstanding_amount) / 100.0 AS aum_crores
    FROM loan_portfolio
    WHERE status IN ('Active', 'NPA')
    GROUP BY year, month
)
SELECT
    pl.year,
    pl.month,
    pl.year || '-' || PRINTF('%02d', pl.month)         AS period,
    pl.net_interest_income                              AS nii_crores,
    ROUND(ma.aum_crores, 2)                             AS aum_crores,
    -- NII is in ₹ Crores; AUM (sum of outstanding_amount/100) is also in ₹ Crores
    ROUND(100.0 * pl.net_interest_income / NULLIF(ma.aum_crores, 0), 4)
                                                        AS nim_monthly_pct,
    ROUND(100.0 * pl.net_interest_income / NULLIF(ma.aum_crores, 0) * 12, 2)
                                                        AS nim_annualised_pct
FROM profit_loss pl
JOIN monthly_aum ma
  ON pl.year = ma.year AND pl.month = ma.month
ORDER BY pl.year, pl.month;
"""

Q5_COST_TO_INCOME = """
-- Q5: Cost-to-Income Ratio — monthly
-- CTI = Operating Expenses / Net Interest Income * 100
SELECT
    year,
    month,
    year || '-' || PRINTF('%02d', month)          AS period,
    net_interest_income                            AS nii_crores,
    opex                                           AS opex_crores,
    provisions                                     AS provisions_crores,
    pat                                            AS pat_crores,
    net_margin                                     AS net_margin_pct,
    ROUND(100.0 * opex / NULLIF(net_interest_income, 0), 2) AS cost_to_income_pct
FROM profit_loss
ORDER BY year, month;
"""

Q6_PAR = """
-- Q6: Portfolio at Risk (PAR) by DPD bucket — monthly
-- PAR 30 = Outstanding with DPD >= 30 / Total Outstanding * 100
-- PAR 60 = Outstanding with DPD >= 60 / Total Outstanding * 100
-- PAR 90 = Outstanding with DPD >= 90 / Total Outstanding * 100
SELECT
    year,
    month,
    year || '-' || PRINTF('%02d', month)   AS period,
    ROUND(SUM(outstanding_amount) / 100.0, 2)   AS total_aum_crores,

    ROUND(100.0 *
        SUM(CASE WHEN dpd_bucket >= 30 THEN outstanding_amount ELSE 0 END)
        / NULLIF(SUM(outstanding_amount), 0), 2) AS par_30_pct,

    ROUND(100.0 *
        SUM(CASE WHEN dpd_bucket >= 60 THEN outstanding_amount ELSE 0 END)
        / NULLIF(SUM(outstanding_amount), 0), 2) AS par_60_pct,

    ROUND(100.0 *
        SUM(CASE WHEN dpd_bucket >= 90 THEN outstanding_amount ELSE 0 END)
        / NULLIF(SUM(outstanding_amount), 0), 2) AS par_90_pct,

    -- DPD bucket breakdown (₹ Crores)
    ROUND(SUM(CASE WHEN dpd_bucket =   0 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_0_cr,
    ROUND(SUM(CASE WHEN dpd_bucket =  30 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_30_cr,
    ROUND(SUM(CASE WHEN dpd_bucket =  60 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_60_cr,
    ROUND(SUM(CASE WHEN dpd_bucket =  90 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_90_cr,
    ROUND(SUM(CASE WHEN dpd_bucket = 180 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_180_cr,
    ROUND(SUM(CASE WHEN dpd_bucket = 360 THEN outstanding_amount ELSE 0 END) / 100.0, 2) AS dpd_360_cr

FROM loan_portfolio
WHERE status IN ('Active', 'NPA')
GROUP BY year, month
ORDER BY year, month;
"""

Q7_DISBURSEMENT_GROWTH = """
-- Q7: Disbursement growth — MoM % and YoY %
-- Uses LAG window function for prior-period comparisons
WITH monthly_dis AS (
    SELECT
        year,
        month,
        year || '-' || PRINTF('%02d', month)   AS period,
        ROUND(SUM(disbursed_amount) / 100.0, 2) AS disbursement_crores
    FROM loan_portfolio
    GROUP BY year, month
),
with_lag AS (
    SELECT
        *,
        LAG(disbursement_crores, 1)  OVER (ORDER BY year, month) AS prev_month_dis,
        LAG(disbursement_crores, 12) OVER (ORDER BY year, month) AS prev_year_dis
    FROM monthly_dis
)
SELECT
    year,
    month,
    period,
    disbursement_crores,
    prev_month_dis,
    prev_year_dis,
    ROUND(100.0 * (disbursement_crores - prev_month_dis)
          / NULLIF(prev_month_dis, 0), 2)                   AS mom_growth_pct,
    ROUND(100.0 * (disbursement_crores - prev_year_dis)
          / NULLIF(prev_year_dis, 0), 2)                    AS yoy_growth_pct
FROM with_lag
ORDER BY year, month;
"""

Q8_PCR = """
-- Q8: Provision Coverage Ratio (PCR) — monthly trend from risk_metrics
-- PCR = Provisions held / Gross NPA * 100 (pre-computed in risk_metrics table)
-- Also compute cumulative provisions from P&L for cross-check
WITH cumulative_prov AS (
    SELECT
        year,
        month,
        provisions,
        SUM(provisions) OVER (ORDER BY year, month
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_provisions
    FROM profit_loss
)
SELECT
    rm.year,
    rm.month,
    rm.year || '-' || PRINTF('%02d', rm.month) AS period,
    rm.gnpa_pct,
    rm.nnpa_pct,
    rm.pcr_pct                                  AS pcr_from_metrics,
    cp.provisions                               AS monthly_provisions_cr,
    ROUND(cp.cum_provisions, 2)                 AS cumulative_provisions_cr
FROM risk_metrics rm
JOIN cumulative_prov cp
  ON rm.year = cp.year AND rm.month = cp.month
ORDER BY rm.year, rm.month;
"""

Q9_TOP5_GEO_NPA = """
-- Q9 (BONUS): Top 5 geographies by NPA contribution — across full period
-- NPA Contribution % = NPA outstanding in that geo / Total NPA outstanding * 100
WITH geo_npa AS (
    SELECT
        geography,
        ROUND(SUM(outstanding_amount) / 100.0, 2)   AS npa_outstanding_crores,
        COUNT(loan_id)                               AS npa_loan_count
    FROM loan_portfolio
    WHERE status = 'NPA'
    GROUP BY geography
),
total_npa AS (
    SELECT SUM(npa_outstanding_crores) AS total FROM geo_npa
)
SELECT
    g.geography,
    g.npa_outstanding_crores,
    g.npa_loan_count,
    ROUND(100.0 * g.npa_outstanding_crores / t.total, 2) AS npa_contribution_pct,
    RANK() OVER (ORDER BY g.npa_outstanding_crores DESC) AS npa_rank
FROM geo_npa g, total_npa t
ORDER BY npa_contribution_pct DESC
LIMIT 5;
"""

# ── Registry (used by kpi_engine.py) ─────────────────────────────────────────
QUERIES = {
    "aum_trend"              : Q1_AUM_TREND,
    "npa_trend"              : Q2_NPA_TREND,
    "collection_efficiency"  : Q3_COLLECTION_EFFICIENCY,
    "nim"                    : Q4_NIM,
    "cost_to_income"         : Q5_COST_TO_INCOME,
    "par"                    : Q6_PAR,
    "disbursement_growth"    : Q7_DISBURSEMENT_GROWTH,
    "pcr"                    : Q8_PCR,
    "top5_geo_npa"           : Q9_TOP5_GEO_NPA,
}

if __name__ == "__main__":
    print("sql_queries.py — Query registry loaded:")
    for name, sql in QUERIES.items():
        lines = [l.strip() for l in sql.strip().splitlines() if l.strip()]
        print(f"  [{name}]  →  {lines[0]}")
