SELECT
    treaty_id,
    reporting_date,
    reporting_quarter,
    profitability_state,
    csm_amount,
    cf_amount
FROM {{ ref('int_treaty_bs_state') }}
WHERE aoc_step = 'CSM_RELEASE'
AND (
    -- Rule 1: wrong quarter
    reporting_quarter NOT IN (2, 4)
    OR
    -- Rule 2: release on onerous
    (profitability_state = 'ONEROUS' AND ABS(csm_amount) > 0.01)
    OR
    -- Rule 3: cf should be 0
    ABS(cf_amount) > 0.01
)