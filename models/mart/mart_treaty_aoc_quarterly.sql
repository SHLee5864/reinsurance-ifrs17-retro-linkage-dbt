-- models/mart_treaty_aoc_quarterly.sql
with bs as (
    select
        t.treaty_id,
        t.goc_id,
        t.treaty_type,          -- assumed / retro (treaty_master 기준)
        s.reporting_date,
        s.aoc_step,
        s.aoc_order,
        s.cf_amount,
        s.csm_amount,
        s.lc_amount,
        s.profitability_state
    from {{ ref('int_treaty_bs_state') }} s
    left join {{ ref('stg_treaty_master') }} t
      on s.treaty_id = t.treaty_id
)

select
    treaty_id,
    goc_id,
    treaty_type,
    reporting_date,
    aoc_step,
    cf_amount,
    csm_amount,
    lc_amount,
    profitability_state
from bs
order by treaty_id, reporting_date, aoc_order