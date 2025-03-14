-- Purpose: Calculate the number of new logos by fiscal year for each company.
WITH customer_first_payment AS (
    SELECT
        company,
        customer_id,
        MIN(payment_month) AS first_payment_month
    FROM
        {{ ref('int_tables_combined') }}
    GROUP BY
        company, customer_id
),
new_logos_by_fy AS (
    SELECT
        company,
        customer_id,
        first_payment_month,
        CASE
            WHEN EXTRACT(MONTH FROM first_payment_month) >= 6 THEN EXTRACT(YEAR FROM first_payment_month) 
            ELSE EXTRACT(YEAR FROM first_payment_month) - 1
        END AS fiscal_year
    FROM
        customer_first_payment
),
new_logo_summary AS (
    SELECT
        company,
        fiscal_year,
        COUNT(DISTINCT customer_id) AS new_logos
    FROM
        new_logos_by_fy
    GROUP BY
        company, fiscal_year
)
SELECT
    company,
    fiscal_year,
    new_logos
FROM
    new_logo_summary
ORDER BY
    company,
    fiscal_year;