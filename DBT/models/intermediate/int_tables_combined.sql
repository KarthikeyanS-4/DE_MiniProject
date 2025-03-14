WITH products AS (
    SELECT
        product_id,
        product_family,
        product_sub_family
    FROM
        {{ ref('stg_ol_products') }}
),
customers AS (
    SELECT
        company,
        customer_id,
        customer_name
    FROM
        {{ ref('stg_ol_customers') }}
),
regions AS (
    SELECT
        customer_id,
        country,
        region
    FROM
        {{ ref('stg_ol_region') }}
),
transactions AS (
    SELECT
        customer_id,
        product_id,
        payment_month,
        revenue_type,
        revenue,
        quantity
    FROM
        {{ ref('stg_ol_transactions') }}
)

SELECT
    t.customer_id,
    c.customer_name,
    r.country,
    r.region,
    t.product_id,
    p.product_family,
    p.product_sub_family,
    t.payment_month,
    t.revenue_type,
    t.revenue,
    t.quantity,
    t.revenue * t.quantity AS total_revenue,
    c.company
FROM
    transactions t
JOIN
    customers c ON t.customer_id = c.customer_id
JOIN
    regions r ON t.customer_id = r.customer_id
JOIN
    products p ON t.product_id = p.product_id