{{ config(
    materialized='incremental',
    on_schema_change='fail'
    )
}}

with test_summary as
(
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_WITH_HOSTS_LISTING' as module_name,
    'ACCEPTED_VALUES_DIM_LISTINGS_WITH_HOSTS_LISTING_ID__1__2__3' as test_name,
    'ACCEPTED_VALUES' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.ACCEPTED_VALUES_DIM_LISTINGS_WITH_HOSTS_LISTING_ID__1__2__3
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_WITH_HOSTS_LISTING' as module_name,
    'NOT_NULL_DIM_LISTINGS_WITH_HOSTS_LISTING_ID' as test_name,
    'NOT_NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_DIM_LISTINGS_WITH_HOSTS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as module_name,
    'RELATIONSHIPS_FCT_REVIEWS' as test_name,
    'RELATIONSHIPS' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.RELATIONSHIPS_FCT_REVIEWS_2F397514D7615E5AE30D8FCE9F0EA58D
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as module_name,
    'NOT_NULL_FCT_REVIEWS_LISTING_ID' as test_name,
    'NOT_NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_FCT_REVIEWS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS' as module_name,
    'UNIQUE_DIM_LISTINGS_WITH_HOSTS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_DIM_LISTINGS_WITH_HOSTS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as module_name,
    'UNIQUE_FCT_REVIEWS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_FCT_REVIEWS_LISTING_ID
)
select * from test_summary