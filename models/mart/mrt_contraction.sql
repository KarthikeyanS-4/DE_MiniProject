WITH revenue_by_period AS (
    SELECT
        customer_id,
        company,
        product_id,
        DATE_TRUNC('month', payment_month) AS payment_period,
        SUM(revenue) AS total_revenue
    FROM
        {{ref("int_tables_combined")}}
    WHERE
        revenue_type = 1
    GROUP BY
        customer_id, company, product_id, DATE_TRUNC('month', payment_month)
),
revenue_comparison AS (
    SELECT
        customer_id,
        company,
        product_id,
        payment_period,
        total_revenue,
        LAG(total_revenue) OVER (
            PARTITION BY customer_id, company, product_id 
            ORDER BY payment_period
        ) AS previous_period_revenue
    FROM
        revenue_by_period
),
revenue_contraction AS (
    SELECT
        company,
        product_id,
        payment_period,
        SUM(
            CASE
                WHEN previous_period_revenue > total_revenue THEN previous_period_revenue - total_revenue
                ELSE 0
            END
        ) AS revenue_contraction
    FROM
        revenue_comparison
    GROUP BY
        company, product_id, payment_period
)
SELECT
    company,
    SUM(revenue_contraction)
FROM
    revenue_contraction
GROUP BY
    company
