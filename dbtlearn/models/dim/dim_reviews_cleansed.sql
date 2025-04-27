{{ config(
    materialized='view'
    )
}}

with SRC_REVIEWS AS
(
select
    *
from {{ref('src_reviews')}}
)
SELECT
 listing_id,
 review_date,
 reviewer_name,
 review_text,
 review_sentiment
FROM
 src_reviews









