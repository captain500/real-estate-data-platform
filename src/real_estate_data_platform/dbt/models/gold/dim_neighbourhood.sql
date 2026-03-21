-- Gold layer: neighbourhood dimension.
-- INSERT-only — if the neighbourhood already exists it is not overwritten.
-- Surrogate key generated via md5(neighbourhood || '~' || city).

{{
    config(
        materialized='incremental',
        unique_key='neighbourhood_sk',
        on_schema_change='append_new_columns',
    )
}}

SELECT
    md5(neighbourhood || '~' || city)  AS neighbourhood_sk,
    neighbourhood,
    city,
    walk_score,
    transit_score,
    bike_score,
    loaded_at
FROM {{ source('silver', 'neighbourhoods') }}

{% if is_incremental() %}
WHERE md5(neighbourhood || '~' || city) NOT IN (
    SELECT neighbourhood_sk FROM {{ this }}
)
{% endif %}
