WITH source_data AS(
    SELECT
        *
    FROM
        {{ source('raw_data', 'ol_region') }}
),
cleaned_data AS(
    SELECT
        DISTINCT customer_id, country, region
    FROM
        source_data
    WHERE
        customer_id IS NOT NULL
        AND country IS NOT NULL
        AND region IS NOT NULL
),
type_conversion AS (
    SELECT
        {{to_varchar('customer_id')}} AS customer_id,
        {{to_varchar('country')}} AS country,
        {{to_varchar('region')}} AS region
    FROM
        cleaned_data
)
SELECT
    *
FROM
    cleaned_data