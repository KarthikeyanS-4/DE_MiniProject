WITH source_data AS (
    SELECT
        *
    FROM
        {{ source('raw_data', 'ol_products') }}
),
remove_nulls AS (
   SELECT
     DISTINCT product_id, product_family, product_sub_family
   FROM
     source_data
   WHERE
    product_id IS NOT NULL
    AND product_family IS NOT NULL
    AND product_sub_family IS NOT NULL
),
datatype_conversion AS (
   SELECT
    {{to_varchar('product_id')}} AS product_id,
    {{to_varchar('product_family')}} AS product_family,
    {{to_varchar('product_sub_family')}} AS product_sub_family
    FROM
      remove_nulls
)

SELECT
  *
FROM
  datatype_conversion