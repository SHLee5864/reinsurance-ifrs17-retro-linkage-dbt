-- models/mart_goc_aoc_quarterly.sql
with bs as (
    select
        g.goc_id,
        g.goc_type,              -- assumed / retro (goc_master 기준)
        s.reporting_date,
        s.aoc_step,
        s.aoc_order,
        s.cf_amount,
        s.cf_cumulative
    from {{ ref('int_goc_bs_state') }} s
    left join {{ ref('stg_goc_master') }} g
      on s.goc_id = g.goc_id
)

select
    goc_id,
    goc_type,
    reporting_date,
    aoc_step,
    aoc_order,
    cf_amount          as movement_amount,
    cf_cumulative      as closing_balance
from bs
order by goc_id, reporting_date, aoc_order
