SELECT
    retro_goc_id,
    retro_treaty_id,
    assumed_treaty_id,
    SUM(lrc_amount) AS total_recovery,
    SUM(ABS(assumed_lc_amount)) * MAX(retro_cession_rate) AS max_allowed_recovery
FROM {{ ref('int_retro_quarterly_movement') }}
GROUP BY retro_goc_id, retro_treaty_id, assumed_treaty_id
HAVING SUM(lrc_amount) > SUM(ABS(assumed_lc_amount)) * MAX(retro_cession_rate) + 0.01