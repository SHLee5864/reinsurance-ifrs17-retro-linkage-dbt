-- int_retro_quarterly_movement
-- Recovery 계산. Delta 기반: LC 변동분에 대해서만 recovery 발생.
-- LC가 증가하는 step (lc_amount < 0)에서만 recovery.

WITH treaty_bs AS (
    SELECT
        treaty_id,
        goc_id,
        reporting_date,
        reporting_quarter,
        aoc_step,
        aoc_order,
        lc_amount
    FROM {{ ref('int_treaty_bs_state') }}
    WHERE lc_amount < 0  -- LC가 증가하는 step만 (delta 기반)
      AND aoc_step IN ('INIT_RECOG', 'VARIANCE')  -- movement step만 (OPENING/CLOSING 제외)
),

retro_links AS (
    SELECT * FROM {{ ref('stg_retro_link') }}
),

retro_treaties AS (
    SELECT
        treaty_id,
        goc_id
    FROM {{ ref('stg_treaty_master') }}
    WHERE treaty_type = 'RETRO'
)

SELECT
    rl.retro_treaty_id,
    rl.assumed_treaty_id,
    rt.goc_id                                AS retro_goc_id,
    tbs.reporting_date,
    tbs.reporting_quarter,
    tbs.aoc_step,
    tbs.aoc_order,
    tbs.lc_amount                            AS assumed_lc_amount,
    rl.retro_cession_rate,
    ABS(tbs.lc_amount) * rl.retro_cession_rate AS lrc_amount
FROM treaty_bs tbs
INNER JOIN retro_links rl
    ON tbs.treaty_id = rl.assumed_treaty_id
INNER JOIN retro_treaties rt
    ON rl.retro_treaty_id = rt.treaty_id