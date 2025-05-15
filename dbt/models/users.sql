{{
    config(
        materialized = "incremental",
        unique_key = "id"
    )
}}

{% if not is_incremental() %}

with ranked as (
    select
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    from {{ source('raw','users')}}
)

SELECT
    id,
    lower(name) as name,
    lower(email) as email,
    lower(address) as address,
    created_at,
    updated_at
from ranked
where rn = 1

{% else %}

SELECT
    id,
    lower(name) as name,
    lower(email) as email,
    lower(address) as address,
    created_at,
    updated_at
from {{ source('raw','users')}}
where updated_at > (
    select max(updated_at) from {{ this }}
)

{% endif %}