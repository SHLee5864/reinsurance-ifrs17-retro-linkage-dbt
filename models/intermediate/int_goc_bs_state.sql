-- int_goc_bs_state
-- BS 관점, GoC level. 이 프로젝트의 핵심 모델.
-- AoC step별 순차적 profitability 판정. CSM/LC 배분.
-- CSM_RELEASE는 이 모델에 없음 (Treaty level에서 수행).

{% macro aoc_order_value(step) %}
    CASE {{ step }}
        WHEN 'OPENING' THEN 1
        WHEN 'INIT_RECOG' THEN 2
        WHEN 'VARIANCE' THEN 3
        WHEN 'CLOSING' THEN 5
    END
{% endmacro %}

WITH goc_movements AS (
    SELECT * FROM {{ ref('int_goc_quarterly_movement') }}
),

-- Step 1: Build INIT_RECOG rows (Q1 only)
init_recog AS (
    SELECT
        goc_id,
        goc_type,
        reporting_date,
        reporting_quarter,
        'INIT_RECOG' AS aoc_step,
        2 AS aoc_order,
        amount AS cf_amount,
        amount AS cf_cumulative,
        -- BS conversion: first determination
        CASE WHEN amount > 0 THEN -1 * amount ELSE 0 END AS csm_amount,
        CASE WHEN amount < 0 THEN amount ELSE 0 END AS lc_amount,
        CASE
            WHEN amount > 0 THEN 'PROFITABLE'
            WHEN amount < 0 THEN 'ONEROUS'
            ELSE 'BREAK_EVEN'
        END AS profitability_state
    FROM goc_movements
    WHERE aoc_step = 'INIT_RECOG'
),

-- Step 2: Compute running cumulative per GoC across quarters
-- First, collect all VARIANCE amounts with their quarter
variance_data AS (
    SELECT
        goc_id,
        goc_type,
        reporting_date,
        reporting_quarter,
        amount AS variance_amount
    FROM goc_movements
    WHERE aoc_step = 'VARIANCE'
),

-- Step 3: Build cumulative state for each quarter's VARIANCE step
-- Need: prior quarter's closing cumulative + current variance
quarterly_state AS (
    SELECT
        v.goc_id,
        v.goc_type,
        v.reporting_date,
        v.reporting_quarter,
        v.variance_amount,
        -- Cumulative = INIT_RECOG + all VARIANCE up to and including this quarter
        ir.cf_cumulative + SUM(v.variance_amount) OVER (
            PARTITION BY v.goc_id
            ORDER BY v.reporting_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cf_cumulative_after_variance,
        -- Previous quarter's closing cumulative (before this variance)
        ir.cf_cumulative + COALESCE(
            SUM(v.variance_amount) OVER (
                PARTITION BY v.goc_id
                ORDER BY v.reporting_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        ) AS cf_cumulative_before_variance,
        ir.cf_cumulative AS init_recog_cumulative
    FROM variance_data v
    INNER JOIN init_recog ir
        ON v.goc_id = ir.goc_id
),

-- Step 4: Determine profitability state and CSM/LC allocation for each VARIANCE
variance_with_state AS (
    SELECT
        goc_id,
        goc_type,
        reporting_date,
        reporting_quarter,
        'VARIANCE' AS aoc_step,
        3 AS aoc_order,
        variance_amount AS cf_amount,
        cf_cumulative_after_variance AS cf_cumulative,

        -- Previous state (based on cumulative before this variance)
        CASE
            WHEN cf_cumulative_before_variance > 0 THEN 'PROFITABLE'
            WHEN cf_cumulative_before_variance < 0 THEN 'ONEROUS'
            ELSE 'BREAK_EVEN'
        END AS prev_state,

        -- Current state (based on cumulative after this variance)
        CASE
            WHEN cf_cumulative_after_variance > 0 THEN 'PROFITABLE'
            WHEN cf_cumulative_after_variance < 0 THEN 'ONEROUS'
            ELSE 'BREAK_EVEN'
        END AS profitability_state,

        -- Previous CSM/LC cumulative (BS perspective)
        CASE WHEN cf_cumulative_before_variance > 0 THEN -1 * cf_cumulative_before_variance ELSE 0 END AS prev_csm_cumulative,
        CASE WHEN cf_cumulative_before_variance < 0 THEN cf_cumulative_before_variance ELSE 0 END AS prev_lc_cumulative,

        cf_cumulative_before_variance,
        cf_cumulative_after_variance
    FROM quarterly_state
),

-- Step 5: Calculate CSM/LC amounts for VARIANCE step based on transition rules
variance_allocated AS (
    SELECT
        goc_id,
        goc_type,
        reporting_date,
        reporting_quarter,
        aoc_step,
        aoc_order,
        cf_amount,
        cf_cumulative,

        CASE
            -- PROFITABLE → PROFITABLE: variance goes to CSM
            WHEN prev_state IN ('PROFITABLE', 'BREAK_EVEN') AND profitability_state = 'PROFITABLE' THEN
                -1 * cf_amount

            -- PROFITABLE → ONEROUS: CSM exhausted, remainder to LC
            WHEN prev_state IN ('PROFITABLE', 'BREAK_EVEN') AND profitability_state IN ('ONEROUS', 'BREAK_EVEN')
                 AND cf_cumulative_after_variance <= 0 THEN
                -1 * prev_csm_cumulative  -- CSM fully reversed (positive value = CSM reduction)

            -- ONEROUS → ONEROUS: variance goes to LC
            WHEN prev_state = 'ONEROUS' AND profitability_state IN ('ONEROUS', 'BREAK_EVEN') THEN
                0

            -- ONEROUS → PROFITABLE: LC reversed, remainder to CSM
            WHEN prev_state = 'ONEROUS' AND profitability_state = 'PROFITABLE' THEN
                -1 * cf_cumulative_after_variance  -- New CSM = negative of positive cumulative

            ELSE 0
        END AS csm_amount,

        CASE
            -- PROFITABLE → PROFITABLE: no LC
            WHEN prev_state IN ('PROFITABLE', 'BREAK_EVEN') AND profitability_state = 'PROFITABLE' THEN
                0

            -- PROFITABLE → ONEROUS: remainder after CSM exhaustion goes to LC
            WHEN prev_state IN ('PROFITABLE', 'BREAK_EVEN') AND profitability_state IN ('ONEROUS', 'BREAK_EVEN')
                 AND cf_cumulative_after_variance <= 0 THEN
                cf_cumulative_after_variance  -- LC = negative cumulative (already negative)

            -- ONEROUS → ONEROUS: variance goes to LC
            WHEN prev_state = 'ONEROUS' AND profitability_state IN ('ONEROUS', 'BREAK_EVEN') THEN
                cf_amount

            -- ONEROUS → PROFITABLE: LC fully reversed
            WHEN prev_state = 'ONEROUS' AND profitability_state = 'PROFITABLE' THEN
                -1 * prev_lc_cumulative  -- LC reversal (positive value = LC reduction)

            ELSE 0
        END AS lc_amount,

        profitability_state

    FROM variance_with_state
),

-- Step 6: Build OPENING rows (Q2, Q3, Q4 = previous quarter's CLOSING state)
-- For Q1, OPENING is zeros (before INIT_RECOG)
opening_rows AS (
    -- Q1 OPENING: zeros
    SELECT DISTINCT
        goc_id,
        goc_type,
        reporting_date,
        reporting_quarter,
        'OPENING' AS aoc_step,
        1 AS aoc_order,
        CAST(0 AS DECIMAL(18,2)) AS cf_amount,
        CAST(0 AS DECIMAL(18,2)) AS cf_cumulative,
        CAST(0 AS DECIMAL(18,2)) AS csm_amount,
        CAST(0 AS DECIMAL(18,2)) AS lc_amount,
        CAST(NULL AS STRING) AS profitability_state
    FROM init_recog

    UNION ALL

    -- Q2-Q4 OPENING: previous quarter's closing state
    SELECT
        va.goc_id,
        va.goc_type,
        next_q.reporting_date,
        next_q.reporting_quarter,
        'OPENING' AS aoc_step,
        1 AS aoc_order,
        -- OPENING cf_amount = previous closing cumulative
        va.cf_cumulative AS cf_amount,
        va.cf_cumulative AS cf_cumulative,
        -- OPENING CSM/LC = previous closing BS state
        CASE WHEN va.cf_cumulative > 0 THEN -1 * va.cf_cumulative ELSE 0 END AS csm_amount,
        CASE WHEN va.cf_cumulative < 0 THEN va.cf_cumulative ELSE 0 END AS lc_amount,
        va.profitability_state
    FROM variance_allocated va
    INNER JOIN (
        SELECT DISTINCT goc_id, reporting_date, reporting_quarter
        FROM variance_data
    ) next_q
        ON va.goc_id = next_q.goc_id
        AND next_q.reporting_quarter = va.reporting_quarter + 1
),

-- Step 7: Build CLOSING rows (sum of all steps in the quarter)
closing_rows AS (
    -- Q1 CLOSING: INIT_RECOG + VARIANCE
    SELECT
        ir.goc_id,
        ir.goc_type,
        ir.reporting_date,
        ir.reporting_quarter,
        'CLOSING' AS aoc_step,
        5 AS aoc_order,
        ir.cf_amount + va.cf_amount AS cf_amount,
        va.cf_cumulative AS cf_cumulative,
        ir.csm_amount + va.csm_amount AS csm_amount,
        ir.lc_amount + va.lc_amount AS lc_amount,
        va.profitability_state
    FROM init_recog ir
    INNER JOIN variance_allocated va
        ON ir.goc_id = va.goc_id
        AND ir.reporting_quarter = va.reporting_quarter

    UNION ALL

    -- Q2-Q4 CLOSING: OPENING + VARIANCE
    -- But OPENING already carries the cumulative, so CLOSING = latest state after VARIANCE
    SELECT
        va.goc_id,
        va.goc_type,
        va.reporting_date,
        va.reporting_quarter,
        'CLOSING' AS aoc_step,
        5 AS aoc_order,
        va.cf_amount AS cf_amount,  -- only variance movement in this quarter
        va.cf_cumulative AS cf_cumulative,
        -- CLOSING CSM/LC = current BS state based on cumulative
        CASE WHEN va.cf_cumulative > 0 THEN -1 * va.cf_cumulative ELSE 0 END AS csm_amount,
        CASE WHEN va.cf_cumulative < 0 THEN va.cf_cumulative ELSE 0 END AS lc_amount,
        va.profitability_state
    FROM variance_allocated va
    WHERE va.reporting_quarter > 1
),

-- Step 8: Union all steps
all_steps AS (
    SELECT goc_id, goc_type, reporting_date, reporting_quarter, aoc_step, aoc_order,
           cf_amount, cf_cumulative, csm_amount, lc_amount, profitability_state
    FROM opening_rows

    UNION ALL

    SELECT goc_id, goc_type, reporting_date, reporting_quarter, aoc_step, aoc_order,
           cf_amount, cf_cumulative, csm_amount, lc_amount, profitability_state
    FROM init_recog

    UNION ALL

    SELECT goc_id, goc_type, reporting_date, reporting_quarter, aoc_step, aoc_order,
           cf_amount, cf_cumulative, csm_amount, lc_amount, profitability_state
    FROM variance_allocated

    UNION ALL

    SELECT goc_id, goc_type, reporting_date, reporting_quarter, aoc_step, aoc_order,
           cf_amount, cf_cumulative, csm_amount, lc_amount, profitability_state
    FROM closing_rows
)

SELECT
    goc_id,
    goc_type,
    reporting_date,
    reporting_quarter,
    aoc_step,
    aoc_order,
    CAST(cf_amount AS DECIMAL(18,2)) AS cf_amount,
    CAST(cf_cumulative AS DECIMAL(18,2)) AS cf_cumulative,
    CAST(csm_amount AS DECIMAL(18,2)) AS csm_amount,
    CAST(lc_amount AS DECIMAL(18,2)) AS lc_amount,
    profitability_state
FROM all_steps