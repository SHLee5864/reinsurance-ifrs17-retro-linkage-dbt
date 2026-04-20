-- mart_goc_aoc_quarterly
-- GoC × quarter × AoC step 상세 리포트
-- Assumed GoC: csm/lc from int_goc_bs_state
-- Retro GoC: lrc from int_retro_bs_state

{{ config(materialized='table') }}

WITH assumed_bs AS (
    SELECT
        s.goc_id,
        g.goc_type,
        g.scenario_description,
        s.reporting_date,
        s.reporting_quarter,
        s.aoc_step,
        s.aoc_order,
        s.cf_amount,
        s.cf_cumulative,
        s.csm_amount,
        s.lc_amount,
        CAST(NULL AS DECIMAL(18,2)) AS lrc_amount,
        s.profitability_state
    FROM {{ ref('int_goc_bs_state') }} s
    INNER JOIN {{ ref('stg_goc_master') }} g
        ON s.goc_id = g.goc_id
    WHERE g.goc_type = 'ASSUMED'
),

-- CSM_RELEASE: aggregate from treaty level to GoC level
csm_release_by_goc AS (
    SELECT
        goc_id,
        reporting_date,
        reporting_quarter,
        'CSM_RELEASE' AS aoc_step,
        4 AS aoc_order,
        CAST(0 AS DECIMAL(18,2)) AS cf_amount,
        CAST(NULL AS DECIMAL(18,2)) AS cf_cumulative,
        SUM(csm_amount) AS csm_amount,
        CAST(0 AS DECIMAL(18,2)) AS lc_amount,
        CAST(NULL AS DECIMAL(18,2)) AS lrc_amount,
        MAX(profitability_state) AS profitability_state
    FROM {{ ref('int_treaty_bs_state') }}
    WHERE aoc_step = 'CSM_RELEASE'
      AND csm_amount != 0
    GROUP BY goc_id, reporting_date, reporting_quarter
),

assumed_with_release AS (
    SELECT
        goc_id, goc_type, scenario_description,
        reporting_date, reporting_quarter, aoc_step, aoc_order,
        cf_amount, cf_cumulative, csm_amount, lc_amount, lrc_amount, profitability_state
    FROM assumed_bs

    UNION ALL

    SELECT
        cr.goc_id,
        g.goc_type,
        g.scenario_description,
        cr.reporting_date,
        cr.reporting_quarter,
        cr.aoc_step,
        cr.aoc_order,
        cr.cf_amount,
        cr.cf_cumulative,
        cr.csm_amount,
        cr.lc_amount,
        cr.lrc_amount,
        cr.profitability_state
    FROM csm_release_by_goc cr
    INNER JOIN {{ ref('stg_goc_master') }} g
        ON cr.goc_id = g.goc_id
),

retro_bs AS (
    SELECT
        r.retro_goc_id AS goc_id,
        g.goc_type,
        g.scenario_description,
        r.reporting_date,
        r.reporting_quarter,
        r.aoc_step,
        r.aoc_order,
        CAST(0 AS DECIMAL(18,2)) AS cf_amount,
        CAST(NULL AS DECIMAL(18,2)) AS cf_cumulative,
        r.csm_amount,
        r.lc_amount,
        r.lrc_amount,
        r.profitability_state
    FROM {{ ref('int_retro_bs_state') }} r
    INNER JOIN {{ ref('stg_goc_master') }} g
        ON r.retro_goc_id = g.goc_id
)

SELECT
    goc_id,
    goc_type,
    scenario_description,
    reporting_date,
    reporting_quarter,
    aoc_step,
    aoc_order,
    cf_amount,
    cf_cumulative,
    csm_amount,
    lc_amount,
    lrc_amount,
    profitability_state
FROM assumed_with_release

UNION ALL

SELECT
    goc_id,
    goc_type,
    scenario_description,
    reporting_date,
    reporting_quarter,
    aoc_step,
    aoc_order,
    cf_amount,
    cf_cumulative,
    csm_amount,
    lc_amount,
    lrc_amount,
    profitability_state
FROM retro_bs

ORDER BY goc_id, reporting_date, aoc_order