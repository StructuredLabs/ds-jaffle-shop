with supplies as (
    select * from {{ ref('supplies') }}
),

products as (
    select * from {{ ref('products') }}
),

supply_costs as (
    select
        s.product_id,
        date_trunc('quarter', s.supply_date) as quarter,
        date_trunc('year', s.supply_date) as financial_year,
        sum(s.supply_cost) as total_supply_cost
    from supplies s
    group by 1, 2, 3
)

select
    sc.product_id,
    p.product_name,
    sc.quarter,
    sc.financial_year,
    sc.total_supply_cost
from supply_costs sc
join products p on sc.product_id = p.product_id
order by sc.financial_year, sc.quarter, sc.product_id