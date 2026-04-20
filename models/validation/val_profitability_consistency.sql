SELECT
    goc_id,
    reporting_date,
    aoc_step,
    profitability_state,
    csm_amount,
    lc_amount
FROM {{ ref('int_goc_bs_state') }}
WHERE aoc_step = 'CLOSING'
AND (
    (profitability_state = 'PROFITABLE' AND ABS(lc_amount) > 0.01)
    OR
    (profitability_state = 'ONEROUS' AND ABS(csm_amount) > 0.01)
)