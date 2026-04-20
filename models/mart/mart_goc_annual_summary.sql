-- mart_goc_annual_summary
-- 연간 기준 각 GoC의 Opening → Movement → Closing 총액
-- CSM/LC/LRC 포함

{{ config(materialized='table') }}

WITH quarterly AS (
    SELECT * FROM {{ ref('mart_goc_aoc_quarterly') }}
),

-- Opening: Q1의 OPENING 또는 INIT_RECOG (Q1 OPENING은 0이므로 INIT_RECOG 후 상태 사용)
opening_state AS (
    SELECT
        goc_id,
        goc_type,
        scenario_description,
        csm_amount AS opening_csm,
        lc_amount AS opening_lc,
        lrc_amount AS opening_lrc
    FROM quarterly
    WHERE reporting_quarter = 1
      AND aoc_step = 'OPENING'
),

-- Closing: Q4 CLOSING
closing_state AS (
    SELECT
        goc_id,
        csm_amount AS closing_csm,
        lc_amount AS closing_lc,
        lrc_amount AS closing_lrc,
        profitability_state AS final_profitability
    FROM quarterly
    WHERE reporting_quarter = 4
      AND aoc_step = 'CLOSING'
),

-- Annual movements by AoC step type
movements AS (
    SELECT
        goc_id,
        SUM(CASE WHEN aoc_step = 'INIT_RECOG' THEN csm_amount ELSE 0 END) AS init_recog_csm,
        SUM(CASE WHEN aoc_step = 'INIT_RECOG' THEN lc_amount ELSE 0 END) AS init_recog_lc,
        SUM(CASE WHEN aoc_step = 'VARIANCE' THEN csm_amount ELSE 0 END) AS total_variance_csm,
        SUM(CASE WHEN aoc_step = 'VARIANCE' THEN lc_amount ELSE 0 END) AS total_variance_lc,
        SUM(CASE WHEN aoc_step = 'CSM_RELEASE' THEN csm_amount ELSE 0 END) AS total_csm_release,
        SUM(CASE WHEN aoc_step IN ('INIT_RECOG', 'VARIANCE') THEN lrc_amount ELSE 0 END) AS total_lrc
    FROM quarterly
    WHERE aoc_step NOT IN ('OPENING', 'CLOSING')
    GROUP BY goc_id
)

SELECT
    o.goc_id,
    o.goc_type,
    o.scenario_description,

    -- Opening
    o.opening_csm,
    o.opening_lc,
    o.opening_lrc,

    -- Movements
    m.init_recog_csm,
    m.init_recog_lc,
    m.total_variance_csm,
    m.total_variance_lc,
    m.total_csm_release,
    m.total_lrc,

    -- Closing
    c.closing_csm,
    c.closing_lc,
    c.closing_lrc,
    c.final_profitability

FROM opening_state o
INNER JOIN movements m ON o.goc_id = m.goc_id
INNER JOIN closing_state c ON o.goc_id = c.goc_id
ORDER BY o.goc_id