WITH source AS (
    SELECT * FROM {{ source('raw_reinsurance', 'retro_link') }}
)

SELECT
    CAST(assumed_treaty_id AS STRING)    AS assumed_treaty_id,
    CAST(retro_treaty_id AS STRING)      AS retro_treaty_id,
    CAST(retro_cession_rate AS DECIMAL(5,2)) AS retro_cession_rate,
    CAST(effective_date AS DATE)         AS effective_date
FROM source