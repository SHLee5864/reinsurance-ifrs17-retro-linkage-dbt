-- int_retro_bs_state
-- BS 관점, Retro GoC level.
-- Recovery를 CSM/LC/LRC로 전환.
-- 동일한 CSM/LC 판정 macro 적용 가능 (retro도 이론적으로 onerous 가능).

WITH retro_movements AS (
    SELECT * FROM {{ ref('int_retro_quarterly_movement') }}
),

-- Aggregate recovery by Retro GoC × reporting_date × aoc_step
retro_goc_aggregated AS (
    SELECT
        retro_goc_id,
        reporting_date,
        reporting_quarter,
        aoc_step,
        aoc_order,
        SUM(lrc_amount) AS lrc_amount
    FROM retro_movements
    GROUP BY
        retro_goc_id,
        reporting_date,
        reporting_quarter,
        aoc_step,
        aoc_order
)

SELECT
    retro_goc_id,
    reporting_date,
    reporting_quarter,
    aoc_step,
    aoc_order,
    CAST(lrc_amount AS DECIMAL(18,2))       AS lrc_amount,
    -- Recovery = profit for retro → CSM (BS: negative)
    CAST(-1 * lrc_amount AS DECIMAL(18,2))  AS csm_amount,
    -- Retro is generally profitable, LC = 0
    CAST(0 AS DECIMAL(18,2))                AS lc_amount,
    'PROFITABLE'                            AS profitability_state
FROM retro_goc_aggregated