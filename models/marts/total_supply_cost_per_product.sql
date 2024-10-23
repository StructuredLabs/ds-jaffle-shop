with supplies as (
    select * from {{ ref('stg_supplies') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

total_supply_cost as (
    select
        product_id,
        sum(supply_cost) as total_supply_cost
    from supplies
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.product_type,
    coalesce(tsc.total_supply_cost, 0) as total_supply_cost
from products p
left join total_supply_cost tsc on p.product_id = tsc.product_id