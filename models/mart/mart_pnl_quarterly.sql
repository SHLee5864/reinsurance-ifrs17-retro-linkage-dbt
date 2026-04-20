-- models/mart_pnl_quarterly.sql
with goc_bs as (
    select
        g.goc_id,
        g.goc_type,                 -- assumed / retro
        s.reporting_date,
        s.aoc_step,
        s.cf_amount,
        case
            when s.cf_cumulative > 0 then 'PROFITABLE'
            when s.cf_cumulative < 0 then 'ONEROUS'
            else 'BREAK_EVEN'
        end as state
    from {{ ref('int_goc_bs_state') }} s
    left join {{ ref('stg_goc_master') }} g
      on s.goc_id = g.goc_id
),

pnl as (
    select
        reporting_date,
        goc_type,
        sum(
            case
                when aoc_step = 'CSM_RELEASE'
                     and goc_type = 'ASSUMED'
                then cf_amount
                else 0
            end
        ) as insurance_revenue_gross,
        sum(
            case
                when aoc_step = 'VARIANCE'
                     and state = 'ONEROUS'
                     and goc_type = 'ASSUMED'
                     and cf_amount < 0
                then cf_amount
                else 0
            end
        ) as insurance_service_expense_gross,
        sum(
            case
                when aoc_step = 'CSM_RELEASE'
                     and goc_type = 'RETRO'
                then cf_amount
                else 0
            end
        ) as recovery_ceded
    from goc_bs
    group by reporting_date, goc_type
)

select
    reporting_date,
    -- Gross (assumed)
    max(case when goc_type = 'ASSUMED' then insurance_revenue_gross end)          as insurance_revenue_gross,
    max(case when goc_type = 'ASSUMED' then insurance_service_expense_gross end)  as insurance_service_expense_gross,
    -- Ceded (retro)
    max(case when goc_type = 'RETRO' then recovery_ceded end)                     as recovery_ceded,
    -- Net (simple combination)
    max(case when goc_type = 'ASSUMED' then insurance_revenue_gross end)
      + coalesce(max(case when goc_type = 'RETRO' then recovery_ceded end), 0)    as insurance_revenue_net
from pnl
group by reporting_date
order by reporting_date
