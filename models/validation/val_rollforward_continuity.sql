WITH closings AS (
    SELECT
        goc_id,
        reporting_date,
        reporting_quarter,
        csm_amount,
        lc_amount,
        cf_cumulative
    FROM {{ ref('int_goc_bs_state') }}
    WHERE aoc_step = 'CLOSING'
),
openings AS (
    SELECT
        goc_id,
        reporting_date,
        reporting_quarter,
        csm_amount,
        lc_amount,
        cf_cumulative
    FROM {{ ref('int_goc_bs_state') }}
    WHERE aoc_step = 'OPENING'
),
paired AS (
    SELECT
        o.goc_id,
        o.reporting_date AS opening_date,
        o.reporting_quarter AS opening_quarter,
        c.reporting_date AS closing_date,
        c.reporting_quarter AS closing_quarter,
        o.csm_amount AS opening_csm,
        c.csm_amount AS prev_closing_csm,
        o.lc_amount AS opening_lc,
        c.lc_amount AS prev_closing_lc
    FROM openings o
    JOIN closings c
        ON o.goc_id = c.goc_id
        AND c.reporting_quarter = o.reporting_quarter - 1
    WHERE o.reporting_quarter > 1  -- Q1 OPENING은 검증 제외 (init_recog 기반)
)
SELECT *
FROM paired
WHERE ABS(opening_csm - prev_closing_csm) > 0.01
   OR ABS(opening_lc - prev_closing_lc) > 0.01