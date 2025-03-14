
WITH ranked_customers AS (
    SELECT
        customer_id,
        customer_name,
        company,
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
    FROM
        {{ ref('int_tables_combined') }}
    GROUP BY
        customer_id,
        customer_name,
        company
)

SELECT
    customer_id,
    customer_name,
    company,
    total_revenue,
    revenue_rank
FROM
    ranked_customers
ORDER BY
    revenue_rank