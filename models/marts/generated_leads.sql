-- models/marts/generated_leads.sql

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

leads AS (
    SELECT
        customer_id,
        MIN(ordered_at) AS first_order_date,
        COUNT(DISTINCT order_id) AS total_orders
    FROM orders
    WHERE ordered_at >= DATEADD(month, -1, CURRENT_DATE())
    GROUP BY 1
)

SELECT
    l.customer_id,
    l.first_order_date,
    l.total_orders,
    c.customer_name,
    c.email
FROM leads l
LEFT JOIN {{ ref('stg_customers') }} c ON l.customer_id = c.customer_id