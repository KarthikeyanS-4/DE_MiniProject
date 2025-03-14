WITH transformed_data AS (
    SELECT
        company,
        customer_id,
        MIN(payment_month) AS first_payment_month,
        MAX(payment_month) AS last_payment_month
    FROM
        {{ ref('int_tables_combined') }}
    GROUP BY
        company, customer_id
),
churned_n_newlogos AS (
    SELECT
        company,
        COUNT(DISTINCT CASE 
            WHEN first_payment_month >= '2018-01-01' THEN customer_id 
        END) AS new_logo,
        COUNT(DISTINCT CASE 
            WHEN last_payment_month < '2020-06-01' THEN customer_id 
        END) AS churned
    FROM
        transformed_data
    GROUP BY
        company
)

SELECT
    *
FROM
    churned_n_newlogos