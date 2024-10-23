-- models/marts/total_supply_cost_per_product_quarterly.sql

with supplies as (
    select * from {{ ref('stg_supplies') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

supply_costs as (
    select
        product_id,
        date_trunc('quarter', to_date(supply_id, 'YYYYMMDD')) as quarter,
        date_part('year', to_date(supply_id, 'YYYYMMDD')) as financial_year,
        sum(supply_cost) as total_supply_cost
    from supplies
    group by 1, 2, 3
)

select
    sc.quarter,
    sc.financial_year,
    p.product_id,
    p.product_name,
    sc.total_supply_cost
from supply_costs sc
join products p on sc.product_id = p.product_id
order by sc.financial_year, sc.quarter, p.product_id