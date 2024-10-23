with customers as (

    select * from {{ ref('customers') }}

),

arizona_customers as (

    select *
    from customers
    where state = 'Arizona'

)

select * from arizona_customers