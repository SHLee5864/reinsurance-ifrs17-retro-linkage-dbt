SELECT
    retro_treaty_id,
    assumed_treaty_id,
    reporting_date,
    aoc_step,
    assumed_lc_amount,
    retro_cession_rate,
    lrc_amount,
    ABS(assumed_lc_amount) * retro_cession_rate AS expected_lrc
FROM {{ ref('int_retro_quarterly_movement') }}
WHERE ABS(lrc_amount - ABS(assumed_lc_amount) * retro_cession_rate) > 0.01