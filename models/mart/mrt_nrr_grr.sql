WITH revenue_by_period AS (
    SELECT
        customer_id,
        company,
        DATE_TRUNC('month', payment_month) AS period, -- Adjust this to 'month', 'quarter', or 'year'
        revenue
    FROM
        {{ ref('int_tables_combined') }}
    WHERE
        revenue_type = 1
),
revenue_comparison AS (
    SELECT
        company,
        customer_id,
        period,
        revenue AS current_revenue,
        COALESCE(LAG(revenue) OVER (
            PARTITION BY customer_id, company
            ORDER BY period
        ),0) AS previous_revenue
    FROM
        revenue_by_period
)
-- revenue_movements AS (
--     SELECT
--         company,
--         period,
--         SUM(
--             CASE 
--                 WHEN previous_revenue IS NULL THEN 0 -- starting revenue (new customers)
--                 ELSE previous_revenue
--             END
--         ) AS starting_revenue,
--         SUM(
--             CASE 
--                 WHEN current_revenue > previous_revenue THEN current_revenue - previous_revenue -- expansion revenue
--                 ELSE 0
--             END
--         ) AS expansion_revenue,
--         SUM(
--             CASE 
--                 WHEN current_revenue < previous_revenue AND current_revenue > 0 THEN previous_revenue - current_revenue -- contraction revenue
--                 ELSE 0
--             END
--         ) AS contraction_revenue,
--         SUM(
--             CASE 
--                 WHEN current_revenue = 0 AND previous_revenue > 0 THEN previous_revenue -- churned revenue
--                 ELSE 0
--             END
--         ) AS churned_revenue
--     FROM
--         revenue_comparison
--     GROUP BY
--         company, period
-- ),
-- nrr_grr_calculation AS (
--     SELECT
--         company,
--         period,
--         starting_revenue,
--         expansion_revenue,
--         contraction_revenue,
--         churned_revenue,
--         -- NRR formula
--         CASE
--             WHEN starting_revenue = 0 THEN 100
--             ELSE (starting_revenue + expansion_revenue - contraction_revenue - churned_revenue) * 100.0 / starting_revenue
--         END AS nrr,
--         -- GRR formula
--         CASE
--             WHEN starting_revenue = 0 THEN 100
--             ELSE (starting_revenue - contraction_revenue - churned_revenue) * 100.0 / starting_revenue
--         END AS grr
--     FROM
--         revenue_movements
-- )

-- SELECT
--     company,
--     period,
--     ROUND(starting_revenue, 2) AS BOP,
--     ROUND(expansion_revenue, 2) AS expansion_revenue,
--     ROUND(contraction_revenue, 2) AS contraction_revenue,
--     ROUND(churned_revenue, 2) AS churned_revenue,
--     ROUND((starting_revenue + expansion_revenue - contraction_revenue - churned_revenue), 2) AS EOP,
--     ROUND(nrr, 2) AS nrr,
--     ROUND(grr, 2) AS grr
-- FROM
--     nrr_grr_calculation
-- ORDER BY
--     company, period
SELECT
    *
FROM
    revenue_comparison