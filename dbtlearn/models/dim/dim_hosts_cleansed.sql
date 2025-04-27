{{ config(
    materialized='view'
    )
}}

with SRC_HOST as 
(
select
    *
from {{ref('src_hosts')}}
)
select
    HOST_ID,
    NVL(HOST_NAME,'Anonymous') as HOST_NAME,
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT    
from SRC_HOSTS