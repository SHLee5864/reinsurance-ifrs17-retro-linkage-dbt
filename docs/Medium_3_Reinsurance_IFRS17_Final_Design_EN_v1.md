# Reinsurance IFRS 17 — Retro Linkage & Loss Recovery
## Final Design Document v1.0
### Date: 2026-04-20

---

# 1. Project Objective

## 1.1 Business Goal

A reinsurer manages the profitability of contracts assumed from cedants while transferring its own risk exposure through retrocession arrangements.

Under IFRS 17, this structure introduces the following complexities:

- Assumed treaties are grouped into Groups of Contracts (GoC), with profitability measured via CSM or LC
- Loss events can trigger profitability state transitions (CSM → LC, LC → CSM)
- Retro treaties provide loss recovery on assumed business and are measured independently as separate GoCs

This project implements these three structures as a reproducible data pipeline:

1. **Entity Relationship:** Explicitly model the Treaty → GoC → Retro relationship structure
2. **State Management:** Track how CSM/LC state changes quarterly at the GoC level
3. **Loss Recovery:** Demonstrate the calculation path from assumed GoC loss through retro cession to recovery
4. **P&L Impact:** Track CSM/LC/LRC independently for Assumed and Retro GoCs, providing interpretable profitability views

**Core Positioning:**
The focus is not on building an IFRS 17 calculation engine, but on translating the most challenging aspects of IFRS 17 — entity relationships, state transitions, and temporal changes — into a reproducible data pipeline architecture.

## 1.2 Positioning vs Medium-1 & Medium-2 Projects

| Dimension | Medium-1 | Medium-2 | Medium-3 |
|-----------|----------|----------|----------|
| Insurance Line | P&C (Motor) | Life Insurance | Reinsurance (Group Life) |
| Perspective | Backward-looking | Forward-looking | State-tracking |
| Core Engine | Historical aggregation | Cashflow projection | State management + entity linkage |
| Primary Driver | Data patterns | Assumptions | Entity relationships + loss events |
| Main Output | Reserve / loss ratio | BEL & sensitivities | CSM/LC movement + loss recovery |
| Pipeline Pattern | Aggregation | Projection | State transition |
| dbt Technique | Basic models + tests | Layered pipeline + validation | Macros + parameterized AoC logic |

## 1.3 Core Questions This Project Answers

1. How are Treaty and GoC relationships structured?
2. How does CSM/LC move at the GoC level when a loss event occurs?
3. How is loss recovery through retro treaties reflected in this movement?
4. How does retro recovery mitigate assumed business losses, and what is the P&L impact for each GoC?

---

# 2. Project Scope

## 2.1 In Scope

**Product & Structure:**
- Non-participating Quota Share reinsurance treaty
- Assumed business: Group Life portfolio assumed from cedant
- Retrocession: risk transfer from reinsurer to retro counterparty
- This project covers only Reinsurer A's perspective

**Entity Relationship:**
- Assumed Treaty → Assumed GoC mapping
- Retro Treaty → Assumed Treaty linkage (1:1 within QS scope)
- Retro Treaty measured as an independent GoC

**IFRS 17 State Management:**
- GoC-level CSM/LC determination and quarterly state tracking
- Four profitability transition cases:
  - Profitable maintained
  - Profitable → Onerous transition
  - Onerous worsening
  - Onerous → Profitable recovery
- Loss event impact calculation and CSM/LC allocation

**Loss Recovery:**
- Confirmed loss in Assumed GoC → retro cession rate applied → loss recovery calculated
- Recovery impact on Retro GoC profitability

**Valuation & Reporting:**
- Quarterly measurement (Q1–Q4) + annual summary
- CSM release: semi-annual basis (H1: Q2, H2: Q4) — parameterized as reporting assumption
- Independent CSM/LC/LRC tracking for Assumed and Retro GoCs
- P&L impact view (insurance revenue, service expense, recovery)

**Pipeline & Technical:**
- dbt + Databricks reproducible pipeline
- dbt macro for AoC logic parameterization
- Synthetic data, test-case-driven design (few contracts, diversified scenarios)

## 2.2 Out of Scope (by Design)

The following items are intentionally excluded. These are deliberate design decisions to focus on entity relationships, state management, and loss recovery paths — not omissions due to ignorance.

**Contract & Structure:**
- Excess of Loss (XL) retro structures and multi-layer attachment
- Co-insurance structures
- Retro chain beyond Reinsurer A (A → B → C)
- Cedant-side original insurance contract structures
- Multi-currency contracts

**IFRS 17 Calculation:**
- Full AoC waterfall — this project focuses on core AoC steps; financial variance, locked-in vs current rate, etc. are not implemented
- Risk Adjustment (RA) calculation
- Discount rate change impact on OCI
- Premium Allocation Approach (PAA)
- Confidence intervals / stochastic valuation
- GL posting and journal entry generation
- Volume updates (new on existing treaties)
- Derecognition

**Operational:**
- Reinsurance credit risk / counterparty default
- Treaty renewal and contract boundary determination
- Claims handling expense breakdown
- Regulatory capital (SCR) calculation

> "In production, XL layer structures, multi-treaty interactions, full AoC movement, RA calculations, discount rate unwind, and GL posting would be added. This design intentionally simplifies to focus on entity relationships and state management."

---

# 3. Regulatory & Conceptual Context

## 3.1 IFRS 17 Conceptual Flow (within project scope)

```
Treaty Incepted (Assumed Business)
        ↓
Grouped into GoC (profitability classification)
        ↓
Initial Recognition (CSM or LC at inception)
        ↓
Subsequent Measurement (quarterly)
   - Loss events impact GoC state
   - CSM absorbs favorable/unfavorable changes (if profitable)
   - LC recognized when GoC turns onerous
   - LC reversed when GoC improves back to profitable
        ↓
Retro Recovery
   - Assumed GoC loss confirmed
   - Cession rate applied → recovery calculated
   - Recovery reflected in Retro GoC (separate measurement)
        ↓
P&L Impact
   - Gross: assumed business result
   - Ceded: retro recovery effect
   - Net: combined view
```

## 3.2 Measurement Model

This project uses the **General Measurement Model (GMM/BBA)** only.

- Premium Allocation Approach (PAA) is not applied
- GMM is the standard measurement model for long-duration reinsurance contracts; the CSM/LC mechanism operates exclusively under GMM

## 3.3 Key IFRS 17 Concepts Used in This Project

| Concept | Role in This Project |
|---------|---------------------|
| Group of Contracts (GoC) | Primary unit for profitability measurement. Treaties are grouped into GoCs |
| CSM (Contractual Service Margin) | Unearned profit on profitable GoCs. Acts as loss absorber |
| Loss Component (LC) | Expected loss recognized on onerous GoCs |
| AoC (Analysis of Change) | Decomposition of CSM/LC movements by step per quarter |
| Loss Recovery Component (LRC) | Loss amount recovered through retro treaties |
| Ceded Contract | Retro treaty measured as a separate GoC under IFRS 17 |

## 3.4 Important Clarification — Not Implemented (by Design)

| IFRS 17 Element | Reason for Exclusion |
|----------------|----------|
| Risk Adjustment (RA) | Requires separate stochastic/quantile calculation. Excluded to focus on state management |
| Discount rate unwind | Time value of money is not the core question of this project |
| OCI option | P&L vs OCI split for discount rate changes is an accounting policy choice outside design scope |
| Financial variance | Related to interest accretion. Excluded as cashflows are received already discounted |
| Locked-in vs Current rate | Linked to OCI option. Accounting policy choice outside design scope |
| Experience adjustment | One of the full AoC steps. Extensible but excluded from initial implementation |
| Contract boundary | Treaty boundary determination is an underwriting judgment; treated as given input |

> "When LC reversal restores CSM, this model re-recognizes CSM at the point where the cumulative GoC-level balance turns positive. In practice, additional constraints on reversal limits may apply."

---

# 4. Product & Portfolio Definition

## 4.1 Line of Business

- **Group Life (corporate group life insurance)**
- Underlying: corporate group insurance portfolio (employee death coverage)
- Benefit: Death benefit
- Cedant cedes Group Life mortality risk to reinsurer via Quota Share

> Group Life was selected because QS is market standard, and catastrophe events (industrial accidents, large-scale incidents) can realistically cause sudden loss spikes enabling natural CSM ↔ LC transition scenarios. It maintains series continuity with Medium-2 (individual term life) while featuring entirely different product structures and pipeline patterns.

## 4.2 Treaty Type

- Quota Share (proportional reinsurance)
- Cession rate varies by retro treaty (demonstrating recovery structure diversity)
- Excess of Loss structures are out of scope

> "This project covers Quota Share structures only. Each assumed treaty maps to one retro treaty. Multi-layer retro attachment under Excess of Loss is out of scope."

## 4.3 Coverage Period

- Reinsurance treaty period: January 1, 2026 – December 31, 2026 (annual renewal)
- Underlying Group Life coverage: long-term (duration of insured employee tenure)
- GMM application rationale: based on the long-term nature of underlying coverage
- This project covers only the first year measurement period of the treaty
- Quarterly measurement (Q1–Q4) + annual summary

> "The reinsurance treaty period is January–December 2026 (annual renewal), while the underlying Group Life coverage is long-term (employee tenure). GMM application is based on the long-term nature of the underlying coverage."

## 4.4 Reinsurer Perspective

- This project covers only Reinsurer A's perspective
- Cedant-side original insurance contract structures are not covered
- Beyond Reinsurer A in the retro chain (A → B → C), only A is in scope
- From B's perspective, A's retro becomes B's assumed business; B may operate its own retro arrangements, but these do not affect A's pipeline

---

# 5. Entity Relationship Design

## 5.1 Core Entity Relationships

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

Retro GoC (1) ◄── covers ── Assumed Treaty (N) via Retro Treaty
```

## 5.2 Relationship Rules

- One GoC contains one or more treaties
- One treaty belongs to exactly one GoC
- Multiple assumed treaties can be mapped to a single retro treaty (N:1 within QS scope)
- One Retro GoC can cover multiple assumed treaties as underlying
- Retro GoC CSM/LC is measured independently from Assumed GoC
- Retro cession rate is defined at the retro treaty level (uniform for all linked assumed treaties)

## 5.3 GoC Design Principle

GoCs are intentionally designed to demonstrate IFRS 17 profitability state transitions across different scenarios. Each GoC represents a distinct profitability path, differentiated by loss event severity and timing.

## 5.4 GoC Case Definition

| GoC | Initial State | Year-End State | Scenario | Example |
|-----|--------------|----------------|----------|---------|
| A | Profitable (CSM > 0) | Profitable maintained | Baseline. Small losses absorbed by CSM | Minor claims, sufficient CSM buffer |
| B | Profitable (CSM > 0) | Onerous (LC recognized) | Large loss event exhausts CSM | Q2 industrial accident, multiple fatalities |
| C | Onerous (LC > 0) | Onerous worsening | Pre-existing onerous GoC with additional losses | Underpriced portfolio with continued adverse experience |
| D | Onerous (LC > 0) | Profitable recovery (CSM re-recognized) | Favorable experience reverses LC | Better-than-expected claims experience, reserve release |

> "GoC C is classified as onerous from initial recognition. This reflects real-world scenarios including pricing errors at inception, risk underestimation, or strategic portfolio-entry underwriting — cases that occur frequently in practice."

## 5.5 Treaty Composition within GoC

- Each GoC contains 1–2 assumed treaties
- Treaty-level cashflows are aggregated to GoC level for CSM/LC determination

## 5.6 Retro Structure

- Retro treaties map to assumed treaties via retro_link (N:1)
- One Retro GoC covers multiple assumed treaties as underlying
- Retro GoC CSM/LC is measured independently
- Retro GoC generally remains profitable (recovery-receiving structure)
- Retro cession rate is uniform per retro treaty

---

# 6. Assumptions Framework

## 6.1 Assumption Philosophy

This project's assumptions are not actuarial projection assumptions but **parameter sets that drive IFRS 17 measurement**.

Cashflows are received from the upstream actuarial system as already-discounted present values. Traditional actuarial assumptions (mortality, lapse, discount rate) are therefore outside this pipeline's scope.

> "This pipeline receives cashflows from the upstream actuarial system at the treaty × quarter × AoC step level. AoC step amount calculation is the upstream's responsibility. This pipeline aggregates received amounts to GoC level for IFRS 17 measurement and reporting."

> "Cashflow generation logic is out of scope. The projection engine covered in the Medium-2 project corresponds to this upstream role."

## 6.2 Parameter Categories

### Treaty Parameters
- **Coverage period:** 2026-01-01 to 2026-12-31
- **Treaty-GoC mapping:** which treaties belong to which GoC
- **Retro cession rate:** recovery rate per retro treaty (uniform per retro treaty)

### Retro Parameters
- **Retro cession rate:** recovery rate per retro treaty
- **Retro-Treaty mapping:** which retro covers which assumed treaties
- **Retro GoC:** separate GoC for retro treaty measurement

### Initial Recognition Parameters
- **Initial CSM/LC:** CSM or LC amount at GoC inception
- GoC A, B: profitable at inception (CSM > 0)
- GoC C, D: onerous at inception (LC > 0)

### Loss Event Scenarios
- **Quarterly cashflow input:** amount per treaty × quarter × AoC step
- **Loss event timing & severity:** which GoC, which quarter, what magnitude
- These values determine GoC profitability state transitions

### Measurement Parameters
- **Valuation frequency:** quarterly (Q1–Q4)
- **Annual summary:** aggregation of quarterly movements
- **CSM/LC determination:** based on sign of GoC-level cumulative balance (positive = CSM, negative = LC)
- **LC reversal allowed:** LC reversed to CSM when GoC recovers profitability (GoC D scenario)

### Reporting Assumption
- **CSM release frequency:** semi-annual — released at Q2 and Q4 closing
- Frequency is managed as a dbt variable; switchable to annual or quarterly

> "CSM release frequency is parameterized as a reporting assumption. This project applies a semi-annual basis with release at Q2 and Q4 closing. Switching to annual or quarterly reporting requires only a variable change."

### Coverage Unit Parameters
- H1 release ratio: 55% (Q2)
- H2 release ratio: 45% (Q4)
- Managed as dbt variables
- No per-treaty differentiation (simplified for single measurement period)

> "CU reflects the time-proportional distribution of expected mortality coverage service within the treaty period. H1 has higher mortality exposure because the insured population is largest at the start of the year, with gradual attrition through mid-year retirements and resignations."

> "In production, per-treaty CU patterns would be managed in a separate RAW table. This project simplifies to a single measurement period."

## 6.3 Rolling State Convention

```
Q1_closing = Q2_opening
Q2_closing = Q3_opening
Q3_closing = Q4_opening
```

Annual summary:
```
annual_opening  = Q1_opening
annual_closing  = Q4_closing
annual_movement = SUM(Q1 through Q4 movements)
```

---

# 7. Cashflow Input Design

## 7.1 Input Source Convention

This pipeline receives cashflows as input from an upstream actuarial system.

- Cashflows are provided as already-discounted present values
- Discount rate application and PVFCF/RA separation are the upstream's responsibility
- One delivery per quarter is assumed

> "In production, asofday-based incremental processing or data versioning may be required. This project assumes a single delivery per quarter."

## 7.2 Cashflow Input Structure

**Grain:** treaty_id × reporting_date × aoc_step

```
treaty_id           -- FK to treaty master
reporting_date      -- quarter-end date (2026-03-31, 2026-06-30, 2026-09-30, 2026-12-31)
aoc_step            -- AoC step identifier
amount              -- discounted cashflow amount (signed)
```

## 7.3 Sign Convention

```
Amount > 0 → Profit direction (premium received, favorable to GoC)
Amount < 0 → Loss direction (claim payment, unfavorable to GoC)

At GoC level:
SUM > 0  → CSM position (profitable)
SUM < 0  → LC position (onerous)
```

### Balance Sheet Convention
```
CSM = negative (unearned profit)
LC  = negative (expected loss)
LRC = positive (retro recovery amount)
CSM Release = positive (CSM reduction, profit realization)
```

## 7.4 AoC Steps

### Implemented AoC Steps

| AoC Step | Role | RAW Input / Calculated | Timing |
|----------|------|----------------------|--------|
| OPENING | Carries forward previous quarter's CLOSING | Calculated (INT) | Every quarter |
| INIT_RECOG | New contract recognition | RAW Input | Q1 only |
| VARIANCE | Current period experience variance (incl. loss events) | RAW Input | Every quarter |
| CSM_RELEASE | Profit recognition through coverage service | Calculated (INT) | Q2, Q4 (semi-annual) |
| CLOSING | Sum of opening + all movements | Calculated (INT) | Every quarter |

- RAW input AoC steps: **INIT_RECOG, VARIANCE**
- INT calculated AoC steps: **OPENING, CSM_RELEASE, CLOSING**

### Quarterly AoC Step Composition

**Q1:**
```
OPENING (= zeros, before INIT_RECOG)
INIT_RECOG
VARIANCE
CLOSING
```

**Q2:**
```
OPENING (= Q1 CLOSING)
VARIANCE
CSM_RELEASE (H1)
CLOSING
```

**Q3:**
```
OPENING (= Q2 CLOSING)
VARIANCE
CLOSING
```

**Q4:**
```
OPENING (= Q3 CLOSING)
VARIANCE
CSM_RELEASE (H2)
CLOSING
```

### Excluded AoC Steps (by Design)

| Excluded Step | Reason |
|----------|----------|
| Financial variance (interest accretion) | Related to discount rate changes. Cashflows are received already discounted |
| Locked-in vs Current rate | Linked to OCI option. Accounting policy choice outside scope |
| Volume update (new on existing) | Adding new underlying to existing treaties. Complexity vs. contribution tradeoff |
| Derecognition | Contract termination. Not applicable within a 1-year coverage period |

> "AoC steps are parameterized via dbt macros. Additional steps can be added without modifying table structures."

---

# 8. Calculation Flow

## 8.1 Overall Processing Order

```
1. Receive cashflow input (RAW: treaty × reporting_date × aoc_step)
        ↓
2. Aggregate to GoC level (INT: treaty → GoC)
        ↓
3. Determine CSM/LC state (INT: GoC cumulative balance sign)
        ↓
4. Allocate to CSM or LC (INT: based on determination result)
        ↓
5. Push determination down to treaty level + CSM Release (INT)
        ↓
6. Calculate retro recovery (INT: assumed LC delta × cession rate)
        ↓
7. Reflect recovery in Retro GoC (INT: independent CSM/LC measurement)
        ↓
8. Generate AoC reports (MART: GoC × AoC step, Treaty × AoC step)
        ↓
9. Annual summary + P&L view (MART: Q1–Q4 aggregation)
```

## 8.2 CSM/LC Determination Logic

Treaty-level impacts aggregated → GoC-level cumulative balance → sign-based determination → CSM or LC allocation

### State Transition Rules

| prev_state | condition | next_state | CSM action | LC action |
|------------|-----------|------------|------------|-----------|
| PROFITABLE | cumulative > 0 | PROFITABLE | Variance → CSM | — |
| PROFITABLE | cumulative ≤ 0 | ONEROUS | CSM fully exhausted | Remaining loss → LC |
| ONEROUS | cumulative < 0 | ONEROUS | — | Variance → LC |
| ONEROUS | cumulative ≥ 0 | PROFITABLE | Remaining profit → CSM | LC fully reversed |

> These rules apply only at the VARIANCE step in int_goc_bs_state. OPENING, INIT_RECOG, and CLOSING do not trigger transitions. Previous step determinations are never retroactively modified.

## 8.3 Retro Recovery Logic

Recovery is **delta-based**: only LC increases (lc_amount < 0) in each AoC step trigger recovery. Cumulative LC is not used.

```
IF lc_amount < 0 THEN
    lrc_amount = ABS(lc_amount) × retro_cession_rate
ELSE
    lrc_amount = 0
END
```

- Recovery is reflected as income in the Retro GoC
- Retro GoC CSM/LC is determined independently from Assumed GoC
- Retro GoC generally remains profitable (CSM > 0)

> "This rule prevents double recovery. Only current-period LC delta — not cumulative LC — triggers recovery calculation."

## 8.4 CSM Release Logic

Semi-annual reporting basis (Q2, Q4):

```
csm_release = treaty_csm_proxy × cu_ratio
```

- Treaty CSM proxy = GoC CSM allocated by treaty cashflow contribution weight
- CU ratio: H1 = 55%, H2 = 45%
- Release applies only to profitable treaties (CSM < 0 in BS terms)
- Onerous treaties: release = 0

---

# 9. Technology Architecture

## 9.1 Platform

- **Databricks** (Community Edition / Free tier)
- **dbt** (dbt-databricks adapter)
- **Delta Lake** for storage

## 9.2 Architecture

```
RAW Generation     → Databricks Notebook (Python)
STG/INT/VAL/MART   → dbt
Debugging/Viz      → Databricks Notebook
Storage            → Delta Lake
Compute            → Databricks SQL
```

## 9.3 dbt Macro Strategy

The technical differentiator of this project is **AoC logic parameterization via dbt macros**.

- CSM/LC determination logic extracted into macros for reuse across all GoC × quarter calculations
- AoC step additions require only macro extension
- CSM release quarter determined automatically via reporting frequency parameter

### dbt Variables

```
{% set release_quarters = [2, 4] %}      -- semi-annual
{% set cu_h1_ratio = 0.55 %}             -- H1 coverage unit ratio
{% set cu_h2_ratio = 0.45 %}             -- H2 coverage unit ratio
```

### AoC Ordering Macro

An aoc_order macro enforces AoC step execution sequence. String-based ORDER BY is never used.

```
{% macro aoc_order(aoc_step) %}
  CASE {{ aoc_step }}
    WHEN 'OPENING' THEN 1
    WHEN 'INIT_RECOG' THEN 2
    WHEN 'VARIANCE' THEN 3
    WHEN 'CSM_RELEASE' THEN 4
    WHEN 'CLOSING' THEN 5
  END
{% endmacro %}
```

> All window functions and cumulative calculations use ORDER BY reporting_date, aoc_order. Adding an AoC step requires only updating this macro.

Medium-1 and Medium-2 made minimal use of dbt macros. This project demonstrates macro-based code reuse and DRY principles.

## 9.4 Pipeline Pattern Differentiation

| Project | Pipeline Pattern | Core Technique |
|---------|-----------------|----------------|
| Medium-1 | Historical aggregation | Window functions, self-join, log-sum-exp |
| Medium-2 | Forward projection | Recursive state rollforward, scenario expansion (CROSS JOIN) |
| Medium-3 | State transition + entity linkage | dbt macros, parameterized logic, rolling state |

---

# 10. Design Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Cashflow received as upstream input | Pipeline focus is state management, not cashflow generation. Avoids duplication with Medium-2 |
| 2 | Discounted amounts received | Discount logic is upstream responsibility. Re-discounting would duplicate Medium-2 |
| 3 | QS structure only | XL introduces multi-layer attachment complexity. Excluded to focus on entity relationships |
| 4 | Four GoC cases designed | Covers all CSM/LC state transition directions (maintain/transition/worsen/recover) |
| 5 | Group Life selected | QS is market standard; catastrophe events enable realistic CSM↔LC transitions |
| 6 | Semi-annual CSM release | European/North American industry practice. Parameterized for flexibility |
| 7 | Single delivery per quarter | Production asofday-based versioning is out of scope. Extensibility noted |
| 8 | Extensible AoC step structure | dbt macro parameterization; step additions require no table structure changes |
| 9 | LC → CSM recovery allowed | Market trend. Permitted under IFRS 17; reversal limit noted in comments |
| 10 | Reinsurer A perspective only | Full retro chain would explode scope. A's assumed + retro is sufficient |
| 11 | Assumed cession rate excluded | Upstream applies cession rate before delivery; rate is not used in this pipeline |
| 12 | Reporting assumption as dbt variable | Few parameters, no runtime changes; table would be overhead |
| 13 | Unified GoC table (assumed + retro) | Same CSM/LC logic applies to both; enables macro reuse |
| 14 | Cashflow/BS perspective separation | Movement models use cashflow sign; BS state models use balance sheet sign. Clear conversion point |
| 15 | CSM Release at treaty level | Industry practice. CU-based release per treaty, not at GoC level |
| 16 | Sequential AoC step determination | Only VARIANCE can trigger transitions. Previous steps are never retroactively modified |
| 17 | Assumed/Retro models separated | Same macros reused but physically separate management |
| 18 | Treaty period 1Y, underlying coverage long-term | GMM application based on underlying long-term nature. Project covers first year measurement only |
| 19 | CU ratio as dbt variable | Production requires per-treaty RAW table. Simplified for single period |
| 20 | No Gross/Ceded/Net merge model | Assumed and Retro are independent GoCs. Natural separation via goc_type |
| 21 | CSM/LC/LRC columns kept separate | Merging would lose impact source analysis. Per-column tracking is industry standard |
| 22 | aoc_order column enforced | String-based ORDER BY prohibited. Ensures window function accuracy |
| 23 | Delta-based recovery | Recovery triggered only by current-period LC delta, not cumulative LC. Prevents double counting |
| 24 | State transition rules documented | Four transition rules explicitly documented. Implementation maintained in macros |
| 25 | Treaty CSM proxy | GoC CSM allocated to treaties by cashflow contribution weight. Enables interpretable release calculation |
| 26 | assumption_version excluded from STG | No FK references, not used in calculations. Pass-through model unnecessary |
| 27 | Sign convention: cashflow +profit/-loss | Consistent with BS convention. CSM negative, LC negative, LRC positive, CSM release positive |
| 28 | N:1 retro treaty mapping | One retro treaty covers multiple assumed treaties. Cession rate uniform per retro treaty |

---

# 11. Glossary

| Term | Definition |
|------|-----------|
| Assumed Business | Reinsurance contracts assumed from cedants |
| Retrocession (Retro) | Risk transfer from reinsurer to another reinsurer |
| GoC (Group of Contracts) | Primary unit for profitability measurement under IFRS 17 |
| CSM (Contractual Service Margin) | Unearned profit on profitable GoCs |
| LC (Loss Component) | Expected loss recognized on onerous GoCs |
| AoC (Analysis of Change) | Decomposition of CSM/LC movements by step |
| Loss Recovery Component (LRC) | Loss amount recovered through retro treaties |
| Quota Share (QS) | Proportional reinsurance. Risk shared by cession rate |
| GMM/BBA (General Measurement Model) | IFRS 17 default measurement model. CSM/LC mechanism operates here |
| Coverage Unit (CU) | Measure of service provision ratio. Basis for CSM release |
| Cedant | Direct insurer or ceding company. Transfers risk to reinsurer |

---

# 12. RAW Layer Design

> RAW = Entity structure definition + upstream cashflow input. No calculations, no determinations, no aggregations.

## 12.1 RAW Layer Design Principles

**Allowed:**
- Entity definitions (GoC, Treaty, Retro relationships)
- Relationship definitions (Treaty → GoC mapping, Retro → Assumed link)
- Time-series input (quarterly cashflows)
- Governance metadata

**Prohibited:**
- Calculations / aggregations
- Profitability determination (CSM/LC)
- Roll-forward / state transitions
- GoC-level aggregation
- OPENING / CLOSING / CSM_RELEASE generation

## 12.2 RAW Structure — Two Axes

| Axis | Role | Tables |
|------|------|--------|
| Master / Entity | What exists (GoC, Treaty, Retro relationships) | goc_master, treaty_master, retro_link |
| Transaction / Input | When and what AoC amounts were received | cashflow_input |
| Governance | Version management metadata | assumption_version |

## 12.3 Table List

| # | Table | Role | Type |
|---|-------|------|------|
| 1 | `goc_master` | GoC definition (Assumed + Retro unified) | Master |
| 2 | `treaty_master` | Treaty definition (Assumed + Retro unified) | Master |
| 3 | `retro_link` | Assumed ↔ Retro relationship + recovery rate | Master |
| 4 | `cashflow_input` | Treaty × quarter × AoC step amounts | Transaction |
| 5 | `assumption_version` | Version governance metadata | Governance |

## 12.4 Table Schemas

### `goc_master`

- **Grain:** 1 row = 1 GoC
- **PK:** goc_id

```
goc_id                    STRING    -- PK
goc_type                  STRING    -- ASSUMED | RETRO
description               STRING
expected_initial_state    STRING    -- PROFITABLE | ONEROUS (validation only, not used in calculations)
scenario_description      STRING    -- Test scenario description
```

### `treaty_master`

- **Grain:** 1 row = 1 Treaty
- **PK:** treaty_id

```
treaty_id                 STRING    -- PK
treaty_type               STRING    -- ASSUMED | RETRO
goc_id                    STRING    -- FK to goc_master
inception_date            DATE
```

### `retro_link`

- **Grain:** 1 row = 1 Assumed–Retro relationship
- **PK:** (assumed_treaty_id, retro_treaty_id)

```
assumed_treaty_id          STRING    -- FK to treaty_master (ASSUMED)
retro_treaty_id            STRING    -- FK to treaty_master (RETRO)
retro_cession_rate         DECIMAL   -- Recovery rate (retro QS rate)
effective_date             DATE
```

### `cashflow_input`

- **Grain:** 1 row = 1 Treaty × 1 Quarter × 1 AoC Step
- **PK:** (treaty_id, reporting_date, aoc_step)

```
treaty_id                  STRING    -- FK to treaty_master
reporting_date             DATE      -- Quarter-end date
aoc_step                   STRING    -- INIT_RECOG | VARIANCE
amount                     DECIMAL   -- Signed, discounted present value
```

### `assumption_version`

- **Grain:** 1 row = 1 version
- **PK:** version_id

```
version_id                 STRING    -- PK
description                STRING
as_of_date                 DATE
```

## 12.5 Reporting Assumption — dbt Variable

CSM release frequency is managed as a dbt variable, not a separate RAW table.

```
{% set release_quarters = [2, 4] %}  -- semi-annual (Q2, Q4)
```

## 12.6 dbt sources.yml

```yaml
version: 2
sources:
  - name: raw_reinsurance
    schema: reinsurance_ifrs17_raw
    tables:
      - name: goc_master
      - name: treaty_master
      - name: retro_link
      - name: cashflow_input
      - name: assumption_version
```

---

# 13. STG Layer Design

> STG = Standardize RAW data into input contracts consumable by INT. Core role: entity relationship integrity assurance.

## 13.1 Model List

```
stg/
├─ stg_goc_master
├─ stg_treaty_master
├─ stg_retro_link
└─ stg_cashflow_input
```

4 models. `assumption_version` has no FK references and is not used in calculations; no STG model is created.

## 13.2 STG Completion Criteria

- All STG model grains are clear with unique/not_null tests passing
- Entity relationship FK chain (GoC → Treaty → Retro Link) validated
- Any STG model can be joined from INT without additional calculations
- No SUM, CASE WHEN (determination), or window functions in STG SQL

---

# 14. INT Layer Design

> INT = State management engine. Cashflow aggregation, BS conversion, CSM/LC determination, CSM Release, Retro Recovery. The core layer of this project.

## 14.1 Core Principles

- Treaty-level cashflow movements are recorded first, then aggregated to GoC level for profitability determination
- Profitability determination is performed **sequentially per AoC step**
- The only step where profitability can change is **VARIANCE**
- Previous step determinations are never retroactively modified
- CSM Release is performed at **treaty level** (not GoC level)
- Cashflow perspective and Balance Sheet perspective conversion points are explicit
- Assumed business and Retro business are managed in separate models

## 14.2 Model List

```
int/
├─ int_treaty_quarterly_movement      -- Cashflow, Treaty level
├─ int_goc_quarterly_movement         -- Cashflow, GoC level
├─ int_goc_bs_state                   -- BS, GoC level (CSM/LC determination)
├─ int_treaty_bs_state                -- BS, Treaty level (determination + CSM Release)
├─ int_retro_quarterly_movement       -- Recovery calculation (LRC)
└─ int_retro_bs_state                 -- BS, Retro GoC level (CSM/LC/LRC)
```

6 models.

## 14.3 Perspective Transition Flow

```
Treaty CF → GoC CF → GoC BS determination → Treaty BS tagging → Retro Recovery → Retro BS
(cashflow)  (cashflow)  (BS conversion)      (BS + Release)      (Recovery)       (BS)
```

## 14.4 dbt Macros

| Macro | Purpose |
|-------|---------|
| `classify_profitability` | Determines PROFITABLE / ONEROUS / BREAK_EVEN from cumulative balance |
| `allocate_csm_lc` | Allocates variance to CSM or LC based on transition rules |
| `calculate_csm_release` | Computes CU-based CSM release for eligible quarters |
| `aoc_order` | Enforces AoC step execution sequence (1–5) |

---

# 15. VALIDATION Layer Design

> VALIDATION = Proves that INT results comply with IFRS 17 state engine rules. Determines whether intended state transitions and relationships were maintained.

## 15.1 VALIDATION Levels

| Level | Purpose | Description |
|-------|---------|-------------|
| L1 | Invariants (hard rules) | Mathematical/accounting rules that must never break |
| L2 | Cross-entity consistency | Assumed ↔ Retro, Release timing |
| L3 | Scenario intent verification | Whether test cases behaved as designed |

## 15.2 Result Convention

- 0 rows = PASS
- ≥1 row = FAIL (INT logic requires correction)
- Numeric comparisons use tolerance: ABS(A - B) > 0.01

## 15.3 Model List

```
validation/
├─ val_bs_invariants                  -- L1: BS invariants at CLOSING
├─ val_profitability_consistency      -- L1: CSM/LC determination rules
├─ val_rollforward_continuity         -- L1: Inter-quarter continuity
├─ val_retro_recovery_identity        -- L2: Retro recovery formula check
├─ val_release_timing                 -- L2: CSM release rules
├─ val_treaty_goc_reconciliation      -- L2: Treaty ↔ GoC consistency
├─ val_retro_no_double_recovery       -- L2: No duplicate recovery
└─ val_scenario_expectation           -- L3: Scenario design verification
```

8 models.

---

# 16. MART Layer Design

> MART = Transforms INT results into interpretable forms. No calculations. Views for analysis and comparison.

## 16.1 Model List

```
mart/
├─ mart_goc_aoc_quarterly          -- GoC × quarter × AoC step detail
├─ mart_treaty_aoc_quarterly       -- Treaty × quarter × AoC step detail
├─ mart_goc_annual_summary         -- Annual opening / movement / closing per GoC
├─ mart_annual_aoc_detail          -- Annual AoC step breakdown per GoC
└─ mart_pnl_quarterly              -- Quarterly Gross / Ceded / Net P&L view
```

5 models.

---

# 17. Model Count Summary

| Layer | Model Count |
|-------|-------------|
| RAW | 5 tables (Databricks Notebook) |
| STG | 4 models |
| INT | 6 models |
| VALIDATION | 8 models |
| MART | 5 models |
| **Total dbt models** | **23** |

> Medium-2 had 26 models. Medium-3 has 23 with fewer models but higher per-model logic density due to entity relationship and state transition complexity.

---

# Document Status

| Section | Status |
|---------|--------|
| Project Objective | ✅ Confirmed |
| Project Scope | ✅ Confirmed |
| Regulatory Context | ✅ Confirmed |
| Product & Portfolio Definition | ✅ Confirmed |
| Entity Relationship Design | ✅ Confirmed |
| Assumptions Framework | ✅ Confirmed |
| Cashflow Input Design | ✅ Confirmed |
| Calculation Flow | ✅ Confirmed |
| Technology Architecture | ✅ Confirmed |
| Design Decisions Log | ✅ Confirmed (28 decisions) |
| RAW Layer Design | ✅ Confirmed |
| STG Layer Design | ✅ Confirmed |
| INT Layer Design | ✅ Confirmed |
| VALIDATION Layer Design | ✅ Confirmed (8 models) |
| MART Layer Design | ✅ Confirmed (5 models) |
