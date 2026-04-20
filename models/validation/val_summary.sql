SELECT 'val_bs_invariants' AS validation, COUNT(*) AS fail_count
FROM {{ ref('val_bs_invariants') }}
UNION ALL
SELECT 'val_profitability_consistency', COUNT(*)
FROM {{ ref('val_profitability_consistency') }}
UNION ALL
SELECT 'val_rollforward_continuity', COUNT(*)
FROM {{ ref('val_rollforward_continuity') }}
UNION ALL
SELECT 'val_retro_recovery_identity', COUNT(*)
FROM {{ ref('val_retro_recovery_identity') }}
UNION ALL
SELECT 'val_release_timing', COUNT(*)
FROM {{ ref('val_release_timing') }}
UNION ALL
SELECT 'val_treaty_goc_reconciliation', COUNT(*)
FROM {{ ref('val_treaty_goc_reconciliation') }}
UNION ALL
SELECT 'val_retro_no_double_recovery', COUNT(*)
FROM {{ ref('val_retro_no_double_recovery') }}
UNION ALL
SELECT 'val_scenario_expectation', COUNT(*)
FROM {{ ref('val_scenario_expectation') }}