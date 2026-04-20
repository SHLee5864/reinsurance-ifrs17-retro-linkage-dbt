WITH source AS (
    SELECT * FROM {{ source('raw_reinsurance', 'cashflow_input') }}
)

SELECT
    CAST(treaty_id AS STRING)            AS treaty_id,
    CAST(reporting_date AS DATE)         AS reporting_date,
    QUARTER(CAST(reporting_date AS DATE)) AS reporting_quarter,
    CAST(aoc_step AS STRING)             AS aoc_step,
    CAST(amount AS DECIMAL(18,2))        AS amount
FROM source