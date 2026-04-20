WITH treaty_sum AS (
    SELECT
        goc_id,
        reporting_date,
        aoc_step,
        SUM(csm_amount) AS treaty_csm_total,
        SUM(lc_amount) AS treaty_lc_total
    FROM {{ ref('int_treaty_bs_state') }}
    WHERE aoc_step NOT IN ('CSM_RELEASE', 'CLOSING')
    GROUP BY goc_id, reporting_date, aoc_step
)
SELECT
    ts.goc_id,
    ts.reporting_date,
    ts.aoc_step,
    ts.treaty_csm_total,
    gs.csm_amount AS goc_csm,
    ts.treaty_lc_total,
    gs.lc_amount AS goc_lc
FROM treaty_sum ts
JOIN {{ ref('int_goc_bs_state') }} gs
    ON ts.goc_id = gs.goc_id
    AND ts.reporting_date = gs.reporting_date
    AND ts.aoc_step = gs.aoc_step
WHERE ABS(ts.treaty_csm_total - gs.csm_amount) > 0.01
   OR ABS(ts.treaty_lc_total - gs.lc_amount) > 0.01