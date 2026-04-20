-- int_treaty_bs_state
-- BS 관점, Treaty level.
-- GoC 판정 결과를 treaty level로 내림. CSM Release는 이 모델에서 수행.

{% set release_quarters = [2, 4] %}
{% set cu_h1_ratio = 0.55 %}
{% set cu_h2_ratio = 0.45 %}

WITH treaty_movements AS (
    SELECT * FROM {{ ref('int_treaty_quarterly_movement') }}
),

goc_state AS (
    SELECT * FROM {{ ref('int_goc_bs_state') }}
),

-- Treaty cumulative balance for CSM proxy
treaty_cumulative AS (
    SELECT
        treaty_id,
        goc_id,
        treaty_type,
        reporting_date,
        reporting_quarter,
        aoc_step,
        amount,
        SUM(amount) OVER (
            PARTITION BY treaty_id
            ORDER BY reporting_date,
                CASE aoc_step
                    WHEN 'INIT_RECOG' THEN 2
                    WHEN 'VARIANCE' THEN 3
                END
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS treaty_cf_cumulative
    FROM treaty_movements
),

-- GoC state at VARIANCE step (= post-variance determination point)
goc_at_variance AS (
    SELECT
        goc_id,
        reporting_date,
        reporting_quarter,
        cf_cumulative AS goc_cf_cumulative,
        csm_amount AS goc_csm,
        profitability_state
    FROM goc_state
    WHERE aoc_step = 'VARIANCE'
),

-- GoC state at CLOSING step (for previous quarter reference)
goc_at_closing AS (
    SELECT
        goc_id,
        reporting_quarter,
        cf_cumulative AS goc_cf_cumulative,
        csm_amount AS goc_csm,
        profitability_state
    FROM goc_state
    WHERE aoc_step = 'CLOSING'
),

-- ===== OPENING rows =====
opening_q1 AS (
    SELECT DISTINCT
        tc.treaty_id,
        tc.goc_id,
        tc.treaty_type,
        tc.reporting_date,
        1 AS reporting_quarter,
        'OPENING' AS aoc_step,
        1 AS aoc_order,
        CAST(0 AS DECIMAL(18,2)) AS treaty_csm_proxy,
        CAST(0 AS DECIMAL(18,2)) AS cf_amount,
        CAST(0 AS DECIMAL(18,2)) AS csm_amount,
        CAST(0 AS DECIMAL(18,2)) AS lc_amount,
        CAST(NULL AS STRING) AS profitability_state
    FROM treaty_cumulative tc
    WHERE tc.reporting_quarter = 1 AND tc.aoc_step = 'INIT_RECOG'
),

-- Previous quarter's latest treaty cumulative (VARIANCE step)
prev_quarter_treaty AS (
    SELECT
        treaty_id,
        goc_id,
        treaty_type,
        reporting_quarter,
        treaty_cf_cumulative
    FROM treaty_cumulative
    WHERE aoc_step = 'VARIANCE'
),

opening_q2_q4 AS (
    SELECT
        pt.treaty_id,
        pt.goc_id,
        pt.treaty_type,
        curr_var.reporting_date,
        curr_var.reporting_quarter,
        'OPENING' AS aoc_step,
        1 AS aoc_order,
        CASE
            WHEN pg.profitability_state = 'PROFITABLE' AND pg.goc_cf_cumulative != 0 THEN
                pg.goc_csm * (pt.treaty_cf_cumulative / pg.goc_cf_cumulative)
            ELSE 0
        END AS treaty_csm_proxy,
        pt.treaty_cf_cumulative AS cf_amount,
        CASE WHEN pt.treaty_cf_cumulative > 0 THEN -1 * pt.treaty_cf_cumulative ELSE 0 END AS csm_amount,
        CASE WHEN pt.treaty_cf_cumulative < 0 THEN pt.treaty_cf_cumulative ELSE 0 END AS lc_amount,
        pg.profitability_state
    FROM prev_quarter_treaty pt
    INNER JOIN (
        SELECT DISTINCT treaty_id, goc_id, reporting_date, reporting_quarter
        FROM treaty_cumulative
        WHERE reporting_quarter > 1 AND aoc_step = 'VARIANCE'
    ) curr_var
        ON pt.treaty_id = curr_var.treaty_id
        AND curr_var.reporting_quarter = pt.reporting_quarter + 1
    INNER JOIN goc_at_closing pg
        ON pt.goc_id = pg.goc_id
        AND pg.reporting_quarter = pt.reporting_quarter
),

all_openings AS (
    SELECT * FROM opening_q1
    UNION ALL
    SELECT * FROM opening_q2_q4
),

-- ===== INIT_RECOG + VARIANCE rows =====
movement_rows AS (
    SELECT
        tc.treaty_id,
        tc.goc_id,
        tc.treaty_type,
        tc.reporting_date,
        tc.reporting_quarter,
        tc.aoc_step,
        CASE tc.aoc_step
            WHEN 'INIT_RECOG' THEN 2
            WHEN 'VARIANCE' THEN 3
        END AS aoc_order,
        CASE
            WHEN gv.profitability_state = 'PROFITABLE' AND gv.goc_cf_cumulative != 0 THEN
                gv.goc_csm * (tc.treaty_cf_cumulative / gv.goc_cf_cumulative)
            ELSE 0
        END AS treaty_csm_proxy,
        tc.amount AS cf_amount,
        CASE
            WHEN gv.profitability_state = 'PROFITABLE' THEN -1 * tc.amount
            ELSE 0
        END AS csm_amount,
        CASE
            WHEN gv.profitability_state IN ('ONEROUS', 'BREAK_EVEN') THEN tc.amount
            ELSE 0
        END AS lc_amount,
        gv.profitability_state
    FROM treaty_cumulative tc
    INNER JOIN goc_at_variance gv
        ON tc.goc_id = gv.goc_id
        AND tc.reporting_date = gv.reporting_date
),

-- ===== CSM_RELEASE rows (Q2, Q4 only) =====
csm_release_rows AS (
    SELECT
        m.treaty_id,
        m.goc_id,
        m.treaty_type,
        m.reporting_date,
        m.reporting_quarter,
        'CSM_RELEASE' AS aoc_step,
        4 AS aoc_order,
        m.treaty_csm_proxy,
        CAST(0 AS DECIMAL(18,2)) AS cf_amount,
        CASE
            WHEN m.profitability_state = 'PROFITABLE' AND m.treaty_csm_proxy < 0 THEN
                ABS(m.treaty_csm_proxy) *
                CASE
                    WHEN m.reporting_quarter = 2 THEN {{ cu_h1_ratio }}
                    WHEN m.reporting_quarter = 4 THEN {{ cu_h2_ratio }}
                END
            ELSE 0
        END AS csm_amount,
        CAST(0 AS DECIMAL(18,2)) AS lc_amount,
        m.profitability_state
    FROM movement_rows m
    WHERE m.reporting_quarter IN ({{ release_quarters | join(',') }})
      AND m.aoc_step = 'VARIANCE'
),

-- ===== CLOSING rows (one per treaty × quarter) =====
all_pre_closing AS (
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, cf_amount, csm_amount, lc_amount, profitability_state, treaty_csm_proxy
    FROM all_openings
    UNION ALL
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, cf_amount, csm_amount, lc_amount, profitability_state, treaty_csm_proxy
    FROM movement_rows
    UNION ALL
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, cf_amount, csm_amount, lc_amount, profitability_state, treaty_csm_proxy
    FROM csm_release_rows
),

closing_rows AS (
    SELECT
        treaty_id,
        goc_id,
        treaty_type,
        reporting_date,
        reporting_quarter,
        'CLOSING' AS aoc_step,
        5 AS aoc_order,
        MAX(CASE WHEN aoc_step = 'VARIANCE' THEN treaty_csm_proxy ELSE NULL END) AS treaty_csm_proxy,
        SUM(cf_amount) AS cf_amount,
        SUM(csm_amount) AS csm_amount,
        SUM(lc_amount) AS lc_amount,
        MAX(CASE WHEN aoc_step = 'VARIANCE' THEN profitability_state END) AS profitability_state
    FROM all_pre_closing
    GROUP BY treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter
),

-- ===== Final union =====
all_steps AS (
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, aoc_order, treaty_csm_proxy, cf_amount, csm_amount, lc_amount, profitability_state
    FROM all_openings
    UNION ALL
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, aoc_order, treaty_csm_proxy, cf_amount, csm_amount, lc_amount, profitability_state
    FROM movement_rows
    UNION ALL
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, aoc_order, treaty_csm_proxy, cf_amount, csm_amount, lc_amount, profitability_state
    FROM csm_release_rows
    UNION ALL
    SELECT treaty_id, goc_id, treaty_type, reporting_date, reporting_quarter,
           aoc_step, aoc_order, treaty_csm_proxy, cf_amount, csm_amount, lc_amount, profitability_state
    FROM closing_rows
)

SELECT
    treaty_id,
    goc_id,
    treaty_type,
    reporting_date,
    reporting_quarter,
    aoc_step,
    aoc_order,
    CAST(treaty_csm_proxy AS DECIMAL(18,2)) AS treaty_csm_proxy,
    CAST(cf_amount AS DECIMAL(18,2)) AS cf_amount,
    CAST(csm_amount AS DECIMAL(18,2)) AS csm_amount,
    CAST(lc_amount AS DECIMAL(18,2)) AS lc_amount,
    profitability_state
FROM all_steps