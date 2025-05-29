{{ config(
    materialized='view',
    tags=["DIM", "HOSTS"]
    )
}}

WITH src_host AS (
  SELECT *
  FROM {{ ref('src_hosts') }}
)

SELECT
  host_id,
  is_superhost,
  created_at,
  updated_at,
  COALESCE(host_name, 'Anonymous') AS host_name
FROM src_hosts
