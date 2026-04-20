WITH source AS (
    SELECT * FROM {{ source('raw_reinsurance', 'treaty_master') }}
)

SELECT
    CAST(treaty_id AS STRING)        AS treaty_id,
    CAST(treaty_type AS STRING)      AS treaty_type,
    CAST(goc_id AS STRING)           AS goc_id,
    CAST(inception_date AS DATE)     AS inception_date,
    QUARTER(CAST(inception_date AS DATE)) AS inception_quarter
FROM source