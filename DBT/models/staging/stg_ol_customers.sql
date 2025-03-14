WITH source_data AS (
    SELECT
        *
    FROM
        {{ source('raw_data', 'ol_customers') }}
),
remove_nulls AS (
   SELECT
     DISTINCT company, customer_id, customername
   FROM
     source_data
   WHERE
    company IS NOT NULL
    AND customer_id IS NOT NULL
    AND customername IS NOT NULL
),
datatype_conversion AS (
   SELECT
    {{to_varchar('company')}} AS company,
    {{to_int('customer_id')}} AS customer_id,
    {{to_varchar('customername')}} AS customer_name
    FROM
      remove_nulls
)

SELECT
  *
FROM
  datatype_conversion