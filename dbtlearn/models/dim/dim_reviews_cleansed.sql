{{ config(
    materialized='view',
    tags=["DIM", "REVIEUE"]  
    )
}}

WITH src_reviews AS (
  SELECT *
  FROM {{ ref('src_reviews') }}
)

SELECT
  listing_id,
  review_date,
  reviewer_name,
  review_text,
  review_sentiment
FROM
  src_reviews
