-- mart_pnl_quarterly
-- Quarterly Gross / Ceded / Net P&L view
-- Insurance revenue = CSM release (assumed, treaty level aggregated)
-- Insurance service expense = onerous loss (assumed VARIANCE, negative cf)
-- Recovery = retro LRC amount

{{ config(materialized='table') }}

WITH -- Assumed: CSM release from treaty level
csm_release AS (
    SELECT
        reporting_date,
        reporting_quarter,
        SUM(csm_amount) AS insurance_revenue_gross
    FROM {{ ref('int_treaty_bs_state') }}
    WHERE aoc_step = 'CSM_RELEASE'
    GROUP BY reporting_date, reporting_quarter
),

-- Assumed: onerous loss (VARIANCE on onerous GoCs)
service_expense AS (
    SELECT
        s.reporting_date,
        s.reporting_quarter,
        SUM(s.lc_amount) AS insurance_service_expense_gross
    FROM {{ ref('int_goc_bs_state') }} s
    WHERE s.aoc_step = 'VARIANCE'
      AND s.lc_amount < 0
    GROUP BY s.reporting_date, s.reporting_quarter
),

-- Retro: recovery (LRC)
retro_recovery AS (
    SELECT
        reporting_date,
        reporting_quarter,
        SUM(lrc_amount) AS recovery_ceded
    FROM {{ ref('int_retro_bs_state') }}
    GROUP BY reporting_date, reporting_quarter
),

-- All quarter-end dates
quarters AS (
    SELECT DISTINCT reporting_date, reporting_quarter
    FROM {{ ref('int_goc_bs_state') }}
    WHERE aoc_step = 'CLOSING'
)

SELECT
    q.reporting_date,
    q.reporting_quarter,
    COALESCE(cr.insurance_revenue_gross, 0) AS insurance_revenue_gross,
    COALESCE(se.insurance_service_expense_gross, 0) AS insurance_service_expense_gross,
    COALESCE(rr.recovery_ceded, 0) AS recovery_ceded,
    COALESCE(cr.insurance_revenue_gross, 0)
        + COALESCE(se.insurance_service_expense_gross, 0)
        + COALESCE(rr.recovery_ceded, 0) AS net_result
FROM quarters q
LEFT JOIN csm_release cr
    ON q.reporting_date = cr.reporting_date
LEFT JOIN service_expense se
    ON q.reporting_date = se.reporting_date
LEFT JOIN retro_recovery rr
    ON q.reporting_date = rr.reporting_date
ORDER BY q.reporting_date