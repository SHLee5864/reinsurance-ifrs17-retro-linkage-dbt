-- int_treaty_quarterly_movement
-- Cashflow 관점, Treaty level.
-- RAW cashflow에 GoC 매핑만 추가. 금액 변환 없음.

WITH cashflows AS (
    SELECT * FROM {{ ref('stg_cashflow_input') }}
),

treaties AS (
    SELECT
        treaty_id,
        treaty_type,
        goc_id
    FROM {{ ref('stg_treaty_master') }}
)

SELECT
    c.treaty_id,
    t.goc_id,
    t.treaty_type,
    c.reporting_date,
    c.reporting_quarter,
    c.aoc_step,
    c.amount
FROM cashflows c
INNER JOIN treaties t
    ON c.treaty_id = t.treaty_id