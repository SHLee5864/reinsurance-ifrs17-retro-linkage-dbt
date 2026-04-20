SELECT
    g.goc_id,
    g.expected_initial_state,
    g.scenario_description,
    s.profitability_state AS actual_final_state
FROM {{ ref('stg_goc_master') }} g
JOIN {{ ref('int_goc_bs_state') }} s
    ON g.goc_id = s.goc_id
WHERE s.reporting_date = '2026-12-31'
    AND s.aoc_step = 'CLOSING'
    AND g.goc_type = 'ASSUMED'
    AND (
        (g.goc_id = 'GOC_A' AND s.profitability_state <> 'PROFITABLE')
        OR (g.goc_id = 'GOC_B' AND s.profitability_state <> 'ONEROUS')
        OR (g.goc_id = 'GOC_C' AND s.profitability_state <> 'ONEROUS')
        OR (g.goc_id = 'GOC_D' AND s.profitability_state <> 'PROFITABLE')
    )