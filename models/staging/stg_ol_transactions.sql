WITH source_data AS (
    SELECT
        *
    FROM
        {{ source('raw_data', 'ol_transactions') }}
),
remove_nulls AS (
   SELECT
     *
   FROM
     source_data
   WHERE
    product_id IS NOT NULL
    AND customer_id IS NOT NULL
),
datatype_conversion AS (
   SELECT
    {{to_int('customer_id')}} AS customer_id,
    {{to_varchar('product_id')}} AS product_id,
    {{to_date('payment_month')}} AS payment_month,
    {{to_int('revenue_type')}} AS revenue_type,
    {{to_float('revenue')}} AS revenue,
    {{to_int('quantity')}} AS quantity
    FROM
      remove_nulls
)

SELECT
  *
FROM
  datatype_conversion