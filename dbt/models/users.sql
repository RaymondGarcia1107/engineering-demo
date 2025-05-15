{{
    config(
        materialized = "incremental",
        unique_key = "id",
        schema = "analytics"
    )
}}

SELECT
    id,
    lower(name),
    email,
    address,
    created_at,
    updated_at
FROM
    {{ source('raw','users')}}

{% if is_incremental() %}
WHERE updated_at > (
    SELECT MAX(updated_at) FROM {{this}}
)
{% end if %}