-- models/marts/generated_leads.sql

with leads as (
    select * from {{ source('ecom', 'leads') }}
),

leads_last_month as (
    select
        lead_id,
        lead_name,
        lead_generated_date
    from leads
    where lead_generated_date >= dateadd(month, -1, current_date())
)

select
    count(distinct lead_id) as lead_count
from leads_last_month