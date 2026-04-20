WITH source AS (
    SELECT * FROM {{ source('raw_reinsurance', 'goc_master') }}
)

SELECT
    CAST(goc_id AS STRING)                   AS goc_id,
    CAST(goc_type AS STRING)                 AS goc_type,
    CAST(expected_initial_state AS STRING)    AS expected_initial_state,
    CAST(scenario_description AS STRING)     AS scenario_description
FROM source