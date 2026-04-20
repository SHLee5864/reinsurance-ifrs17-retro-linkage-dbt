-- models/mart_goc_annual_summary.sql
with bs as (
    select *
    from {{ ref('int_goc_bs_state') }}
),

opening as (
    select
        goc_id,
        min(reporting_date) as first_reporting_date
    from bs
    group by goc_id
),

closing as (
    select
        goc_id,
        max(reporting_date) as last_reporting_date
    from bs
    group by goc_id
),

opening_balance as (
    select
        b.goc_id,
        b.cf_cumulative as annual_opening
    from bs b
    join opening o
      on b.goc_id = o.goc_id
     and b.reporting_date = o.first_reporting_date
     and b.aoc_step = 'OPENING'
),

closing_balance as (
    select
        b.goc_id,
        b.cf_cumulative as annual_closing
    from bs b
    join closing c
      on b.goc_id = c.goc_id
     and b.reporting_date = c.last_reporting_date
     and b.aoc_step = 'CLOSING'
),

movement as (
    select
        goc_id,
        sum(cf_amount) as annual_movement
    from bs
    where aoc_step in ('INIT_RECOG', 'VARIANCE', 'CSM_RELEASE')
    group by goc_id
)

select
    o.goc_id,
    o.annual_opening,
    m.annual_movement,
    c.annual_closing
from opening_balance o
join movement m using (goc_id)
join closing_balance c using (goc_id)
order by goc_id
