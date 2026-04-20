SELECT
    goc_id,
    reporting_date,
    cf_cumulative,
    csm_amount,
    lc_amount,
    profitability_state
FROM {{ ref('int_goc_bs_state') }}
WHERE aoc_step = 'CLOSING'
AND (
    -- Profitable: CSM should equal -1 * cf_cumulative
    (profitability_state = 'PROFITABLE'
     AND (ABS(csm_amount - (-1 * cf_cumulative)) > 0.01
          OR ABS(lc_amount) > 0.01))
    OR
    -- Onerous: LC should equal cf_cumulative
    (profitability_state = 'ONEROUS'
     AND (ABS(csm_amount) > 0.01
          OR ABS(lc_amount - cf_cumulative) > 0.01))
    OR
    -- Break-even: both zero
    (profitability_state = 'BREAK_EVEN'
     AND (ABS(csm_amount) > 0.01
          OR ABS(lc_amount) > 0.01))
)