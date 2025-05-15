{{
    config(
        materialized = "incremental",
        unique_key = "id"
    )
}}

{% if not is_incremental() %}

WITH ranked as (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM {{ source('raw','transactions')}}
)

SELECT 
    id,
    user_id,
    amount,
    transaction_type,
    created_at,
    updated_at
FROM ranked
WHERE rn = 1

{% else %}

SELECT
    id, 
    user_id,
    amount,
    transaction_type,
    created_at,
    updated_at
FROM {{ source('raw','transactions')}}
WHERE updated_at > (
    SELECT MAX(updated_at) FROM {{ this }}
)

{% endif %}