select u1.listing_id,
u1.review_text,
u1.reviewer_name,
u1.review_date,
u2.created_at
from  {{ ref('fct_reviews')}} u1
join {{ ref('dim_listings_cleansed')}} u2
on u1.listing_id = u2.listing_id
where u1.review_date < u2.CREATED_AT