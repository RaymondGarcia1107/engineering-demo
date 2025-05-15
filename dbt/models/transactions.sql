{{
    config(
        materialized = "incremental",
        unique_key = "id",
        schema = "analytics"
    )
}}

SELECT 
    id, 
    user_id,
    amount,
    transaction_type,
    created_at,
    updated_at
FROM
    {{ source('raw','transactions') }}
{% if is_incremental() %}
WHERE updated_at > (
    SELECT MAX(updated_at) FROM {{this}}
)
{% end if %}