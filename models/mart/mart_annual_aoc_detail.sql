-- mart_annual_aoc_detail
-- 연간 AoC step별 금액 합산. Delta 설명용.
-- OPENING + INIT_RECOG + VARIANCE + CSM_RELEASE = CLOSING 항등식 검증 가능.

{{ config(materialized='table') }}

WITH quarterly AS (
    SELECT * FROM {{ ref('mart_goc_aoc_quarterly') }}
)

SELECT
    goc_id,
    goc_type,
    aoc_step,
    SUM(csm_amount) AS annual_csm_amount,
    SUM(lc_amount) AS annual_lc_amount,
    SUM(lrc_amount) AS annual_lrc_amount
FROM quarterly
GROUP BY goc_id, goc_type, aoc_step
ORDER BY goc_id,
    CASE aoc_step
        WHEN 'OPENING' THEN 1
        WHEN 'INIT_RECOG' THEN 2
        WHEN 'VARIANCE' THEN 3
        WHEN 'CSM_RELEASE' THEN 4
        WHEN 'CLOSING' THEN 5
    END