{{ config(
    materialized='view',
    tags=["DIM", "LISTINGS"]  
    )
}}

WITH src_listings AS (
  SELECT *
  FROM
    {{ ref('src_listings') }}
)

SELECT
  listing_id,
  listing_name,
  room_type,
  host_id,
  REPLACE(
    price_str,
    '$'
  )::NUMBER(
    10,
    2
  ) AS price,
  created_at,
  updated_at,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights
FROM
  src_listings
