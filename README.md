# Reinsurance IFRS 17 — Retro Linkage & Loss Recovery Pipeline

**Version:** 1.0
**Author:** SukHee Lee
**Date:** April 2026
**Stack:** dbt + Databricks + Delta Lake

---

## Executive Summary

This project implements a data pipeline for **IFRS 17 reinsurance measurement**, focusing on the structural patterns that make reinsurance reporting uniquely complex: entity relationships, profitability state transitions, and loss recovery through retrocession.

The pipeline tracks how a reinsurer's assumed business moves through CSM (Contractual Service Margin) and LC (Loss Component) states quarter by quarter, and how retro treaties provide loss recovery — all modeled as a reproducible dbt pipeline on Databricks.

### What This Pipeline Produces

| Output | Description | MART Model |
|--------|-------------|------------|
| GoC AoC Report | CSM/LC/LRC movement per GoC × quarter × AoC step | mart_goc_aoc_quarterly |
| Treaty AoC Report | Treaty-level movement detail with CSM release | mart_treaty_aoc_quarterly |
| Annual Summary | Opening → movement → closing per GoC | mart_goc_annual_summary |
| Annual AoC Detail | AoC step breakdown for delta explanation | mart_annual_aoc_detail |
| P&L View | Quarterly gross / ceded / net result | mart_pnl_quarterly |

### Who Uses These Outputs

| Team | What They Need | Which Model |
|------|---------------|-------------|
| IFRS 17 Reporting | CSM/LC state and movement per GoC | mart_goc_aoc_quarterly |
| Actuarial | Treaty-level profitability analysis | mart_treaty_aoc_quarterly |
| Finance | Quarterly and annual P&L | mart_pnl_quarterly, mart_goc_annual_summary |
| Risk Management | Retro recovery effectiveness | mart_goc_aoc_quarterly (retro GoC) |
| Management | Portfolio-level profitability overview | mart_goc_annual_summary |

---

## Project Context — Part 3 of 5

This is the third project in a series building toward a comprehensive insurance data platform:

| # | Project | Insurance Line | Pipeline Pattern | Status |
|---|---------|---------------|-----------------|--------|
| 1 | Medium-1 | P&C (Motor) — Loss Reserving | Historical aggregation | ✅ Complete |
| 2 | Medium-2 | Life Insurance — BEL & Sensitivity | Forward projection | ✅ Complete |
| **3** | **Medium-3** | **Reinsurance — IFRS 17 Retro Linkage** | **State transition** | **✅ Current** |
| 4 | Medium-4 | TBD | TBD | Planned |
| 5 | Large | IFRS 17 Analytics Platform on Azure | E2E with CI/CD | Planned |

### What Makes This Project Different

| Dimension | Medium-1 | Medium-2 | Medium-3 |
|-----------|----------|----------|----------|
| Core Engine | Historical aggregation | Cashflow projection | State management + entity linkage |
| Primary Driver | Data patterns | Assumptions | Entity relationships + loss events |
| Main Output | Reserve / loss ratio | BEL & sensitivities | CSM/LC movement + loss recovery |
| dbt Technique | Basic models + tests | Layered pipeline + validation | **Macros + parameterized AoC logic** |

---

## Business Domain

### The Reinsurance Problem

A reinsurer (Company A) assumes risk from cedants and transfers part of that risk through retrocession. Under IFRS 17:

- **Assumed treaties** are grouped into **Groups of Contracts (GoC)**, each measured for profitability
- A profitable GoC carries a **CSM** (unearned profit); an onerous GoC carries a **Loss Component**
- **Loss events** can transition a GoC from profitable to onerous (or vice versa)
- **Retro treaties** recover a portion of losses, measured as a separate GoC

### What This Pipeline Models

Four GoC scenarios demonstrate all profitability transition directions:

| GoC | Start | End | What Happens |
|-----|-------|-----|-------------|
| A | Profitable | Profitable | Small losses absorbed by CSM — baseline |
| B | Profitable | **Onerous** | Q2 industrial accident exhausts CSM → LC recognized |
| C | Onerous | Onerous (worse) | Underpriced portfolio with continued adverse experience |
| D | Onerous | **Profitable** | Q3 favorable experience reverses LC → CSM restored |

### Product Structure

- **Line of Business:** Group Life (corporate employee death coverage)
- **Treaty Type:** Quota Share (proportional reinsurance)
- **Measurement Model:** General Measurement Model (GMM/BBA)
- **Treaty Period:** 2026 (first year measurement)
- **Underlying Coverage:** Long-term (employee tenure) — GMM applied based on this

---

## Pipeline Architecture

```
RAW (Delta tables)  →  STG (views)  →  INT (views)  →  VAL (views)  →  MART (tables)
   5 tables              4 models        6 models        8 models        5 models
```

### Data Flow

```
                    CASHFLOW PERSPECTIVE                    BALANCE SHEET PERSPECTIVE
                    ───────────────────                    ────────────────────────

RAW                 cashflow_input
                         │
STG                 stg_cashflow_input
                         │
INT          int_treaty_quarterly_movement
                         │  (GROUP BY GoC)
             int_goc_quarterly_movement
                         │  (BS conversion)
                                              int_goc_bs_state ──────────────────┐
                                                     │  (push down + release)    │
                                              int_treaty_bs_state                │
                                                     │  (LC delta → recovery)    │
                                              int_retro_quarterly_movement       │
                                                     │  (retro BS)               │
                                              int_retro_bs_state                 │
                                                                                 │
VAL                                           val_bs_invariants                  │
                                              val_profitability_consistency      │
                                              val_rollforward_continuity         │
                                              val_retro_recovery_identity        │
                                              val_release_timing                 │
                                              val_treaty_goc_reconciliation      │
                                              val_retro_no_double_recovery       │
                                              val_scenario_expectation           │
                                                                                 │
MART                                          mart_goc_aoc_quarterly ◄───────────┘
                                              mart_treaty_aoc_quarterly
                                              mart_goc_annual_summary
                                              mart_annual_aoc_detail
                                              mart_pnl_quarterly
```

### Entity Relationships

```
Assumed GoC (1) ── contains ──► Assumed Treaty (N)
                                      │
                                      │ N:1 (QS scope)
                                      ▼
                                Retro Treaty (1)
                                      │
                                      │ belongs to
                                      ▼
                                Retro GoC (1) ── CSM/LC measured independently
```

---

## Key Design Decisions

### Sign Convention

```
CASHFLOW:   + = profit (premium received)     - = loss (claim payment)
BS:         CSM = negative (unearned profit)  LC = negative (expected loss)
            LRC = positive (retro recovery)   CSM Release = positive (profit recognition)
```

### State Transition Rules

Profitability transitions occur **only at the VARIANCE step**. Previous steps are never retroactively modified.

| From | Condition | To | CSM Action | LC Action |
|------|-----------|-----|------------|-----------|
| Profitable | cumulative > 0 | Profitable | Variance → CSM | — |
| Profitable | cumulative ≤ 0 | **Onerous** | CSM fully exhausted | Remainder → LC |
| Onerous | cumulative < 0 | Onerous | — | Variance → LC |
| Onerous | cumulative ≥ 0 | **Profitable** | Remainder → CSM | LC fully reversed |

### CSM Release

- **Level:** Treaty (not GoC) — industry practice
- **Frequency:** Semi-annual (Q2, Q4) — parameterized via dbt variable
- **Basis:** Coverage unit ratio (H1: 55%, H2: 45%)
- **Proxy:** GoC CSM allocated to treaties by cashflow contribution weight

> "In production, per-treaty CU patterns would be managed in a separate RAW table. This project simplifies to a single measurement period with uniform CU ratios."

### Delta-Based Retro Recovery

Recovery is triggered **only by current-period LC delta** (lc_amount < 0), not cumulative LC. This prevents double counting.

```
IF lc_amount < 0 THEN recovery = ABS(lc_amount) × retro_cession_rate
ELSE recovery = 0
```

Only movement steps (INIT_RECOG, VARIANCE) feed into recovery. OPENING and CLOSING are excluded.

---

## Validation Framework

8 validation models across 3 levels. All models return **0 rows = PASS**.

| Level | Model | What It Checks |
|-------|-------|----------------|
| L1 | val_bs_invariants | CSM/LC equals BS conversion of cf_cumulative at CLOSING |
| L1 | val_profitability_consistency | Profitable → LC=0, Onerous → CSM=0 |
| L1 | val_rollforward_continuity | OPENING = previous quarter's CLOSING |
| L2 | val_retro_recovery_identity | LRC = \|Assumed LC\| × cession rate |
| L2 | val_release_timing | CSM release only in Q2/Q4, only for profitable treaties |
| L2 | val_treaty_goc_reconciliation | Treaty-level sum matches GoC-level |
| L2 | val_retro_no_double_recovery | Cumulative recovery ≤ cumulative loss × rate |
| L3 | val_scenario_expectation | GoC A/B/C/D end in designed states |

---

## Simplifications vs. Production

| This Project | Production Reality |
|---|---|
| 4 Assumed GoCs + 1 Retro GoC | Hundreds of GoCs across multiple lines |
| Quota Share only | QS + Excess of Loss + Facultative |
| 5 AoC steps | 15+ AoC steps including financial variance, experience adjustment |
| Cashflows received pre-discounted | Discount rate management, locked-in vs current |
| No Risk Adjustment | RA calculation with confidence levels |
| Single retro treaty per rate | Multi-layer retro programs |
| Semi-annual CSM release, uniform CU | Per-treaty CU patterns, flexible reporting frequency |
| No GL posting | Journal entry generation, sub-ledger integration |
| Synthetic test data (38 cashflow rows) | Production data from actuarial systems |
| dbt variables for parameters | Configuration tables with version control |
| No CI/CD | GitHub Actions → dbt test on PR → Databricks job on merge |

---

## Technology Stack

- **dbt-core** ≥ 1.7
- **dbt-databricks** (production)
- **dbt-utils** — unique_combination_of_columns, generic tests
- **dbt-expectations** — row count validation for validation layer
- **Databricks SQL Warehouse** (Serverless)
- **Delta Lake** — ACID, schema enforcement, versioning
- **Python 3.10+** — RAW data generation (Databricks Notebook)

## Repository Structure

```
reinsurance-ifrs17-retro-linkage-dbt/
├── models/
│   ├── sources/          # sources.yml
│   ├── staging/          # 4 STG models + schema.yml
│   ├── intermediate/     # 6 INT models + schema.yml
│   ├── validation/       # 8 VAL models + schema.yml
│   └── mart/             # 5 MART models + schema.yml
├── macros/               # AoC ordering, CSM/LC determination
├── notebooks/            # RAW data generation (Databricks)
├── dbt_project.yml
├── packages.yml
├── README.md             # English
├── README_FR.md          # French
└── Medium_3_Final_Design_EN_v1.md  # Full design document
```

## Execution

```bash
# 1. Generate RAW data (Databricks Notebook)
#    Run notebooks/generate_raw_data.py in Databricks

# 2. Install dependencies
dbt deps

# 3. Run pipeline
dbt run

# 4. Run tests
dbt test

# 5. Generate documentation
dbt docs generate
dbt docs serve
```

---

## Model Count Summary

| Layer | Count | Materialization |
|-------|-------|----------------|
| RAW | 5 tables | Delta (Databricks Notebook) |
| STG | 4 models | view |
| INT | 6 models | view |
| VALIDATION | 8 models | view |
| MART | 5 models | table |
| **Total** | **23 dbt models** | |

---

## Author

**SukHee Lee** — Actuarial Data Analyst | IFRS 17 · dbt · Databricks
Building insurance data pipelines across reserving, IFRS 17, and analytics engineering workflows.

GitHub: github.com/SHLee5864
Medium: medium.com/@lsh5864