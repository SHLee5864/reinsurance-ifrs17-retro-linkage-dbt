-- int_goc_quarterly_movement
-- Cashflow 관점, GoC level.
-- Treaty → GoC 합산. AoC step grain 유지.

WITH treaty_movements AS (
    SELECT * FROM {{ ref('int_treaty_quarterly_movement') }}
),

gocs AS (
    SELECT
        goc_id,
        goc_type
    FROM {{ ref('stg_goc_master') }}
)

SELECT
    tm.goc_id,
    g.goc_type,
    tm.reporting_date,
    tm.reporting_quarter,
    tm.aoc_step,
    SUM(tm.amount)                    AS amount,
    COUNT(DISTINCT tm.treaty_id)      AS treaty_count
FROM treaty_movements tm
INNER JOIN gocs g
    ON tm.goc_id = g.goc_id
GROUP BY
    tm.goc_id,
    g.goc_type,
    tm.reporting_date,
    tm.reporting_quarter,
    tm.aoc_step