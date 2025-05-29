{{ config(
    materialized='incremental'
    tags=["DIM", "TEST"]  
    )
}}

with test_data_summary as
(
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_WITH_HOSTS_LISTING' as dbt_model,
    'ACCEPTED_VALUES_DIM_LISTINGS_WITH_HOSTS_LISTING_ID__1__2__3' as test_name,
    'ACCEPTED_VALUES' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.ACCEPTED_VALUES_DIM_LISTINGS_WITH_HOSTS_LISTING_ID__1__2__3
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_WITH_HOSTS_LISTING' as dbt_model,
    'NOT_NULL_DIM_LISTINGS_WITH_HOSTS_LISTING_ID' as test_name,
    'NOT_NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_DIM_LISTINGS_WITH_HOSTS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as dbt_model,
    'RELATIONSHIPS_FCT_REVIEWS' as test_name,
    'RELATIONSHIPS' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.RELATIONSHIPS_FCT_REVIEWS_2F397514D7615E5AE30D8FCE9F0EA58D
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as dbt_model,
    'NOT_NULL_FCT_REVIEWS_LISTING_ID' as test_name,
    'NOT_NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_FCT_REVIEWS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS' as dbt_model,
    'UNIQUE_DIM_LISTINGS_WITH_HOSTS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_DIM_LISTINGS_WITH_HOSTS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'FCT_REVIEWS' as dbt_model,
    'UNIQUE_FCT_REVIEWS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_FCT_REVIEWS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_HOSTS_CLEANSED' as dbt_model,
    'NOT_NULL_DIM_HOSTS_CLEANSED_HOST_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_DIM_HOSTS_CLEANSED_HOST_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_CLEANSED' as dbt_model,
    'NOT_NULL_DIM_LISTINGS_CLEANSED_LISTING_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_DIM_LISTINGS_CLEANSED_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_REVIEWS_CLEANSED' as dbt_model,
    'NOT_NULL_DIM_REVIEWS_CLEANSED_LISTING_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_DIM_REVIEWS_CLEANSED_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_HOSTS' as dbt_model,
    'NOT_NULL_SRC_HOSTS_HOST_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_SRC_HOSTS_HOST_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_LISTINGS' as dbt_model,
    'NOT_NULL_SRC_REVIEWS_LISTING_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_SRC_LISTINGS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_REVIEWS' as dbt_model,
    'NOT_NULL_SRC_REVIEWS_LISTING_ID' as test_name,
    'NOT NULL' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as fail_type,
    count(*) as faild_count
from AIRBNB.DEV_DBT_TEST_TABLES.NOT_NULL_SRC_REVIEWS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_HOSTS_CLEANSED' as dbt_model,
    'UNIQUE_DIM_HOSTS_CLEANSED_HOST_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_DIM_HOSTS_CLEANSED_HOST_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_LISTINGS_CLEANSED' as dbt_model,
    'UNIQUE_DIM_LISTINGS_CLEANSED_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_DIM_LISTINGS_CLEANSED_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'DIM_REVIEWS_CLEANSED' as dbt_model,
    'UNIQUE_DIM_REVIEWS_CLEANSED_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_DIM_REVIEWS_CLEANSED_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_HOSTS' as dbt_model,
    'UNIQUE_SRC_HOSTS_HOST_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_SRC_HOSTS_HOST_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_REVIEWS' as dbt_model,
    'UNIQUE_SRC_REVIEWS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_SRC_REVIEWS_LISTING_ID
union all
select 
    current_timestamp as time_stamp,
    'SRC_LISTINGS' as dbt_model,
    'UNIQUE_SRC_LISTINGS_LISTING_ID' as test_name,
    'UNIQUE' as test_type,
    case when count(*)>0 then 'failed' else 'not failed' end as error_type,
    count(*) as error_count
from AIRBNB.DEV_DBT_TEST_TABLES.UNIQUE_SRC_LISTINGS_LISTING_ID
),
finalresult as
(
select  (select max(test_nr)+1 from AIRBNB.DEV.LOAD_TEST_DATA) as test_nr,
        time_stamp,
        dbt_model,
        test_name,
        test_type,
        error_type,
        error_count
from test_data_summary
)

select * from finalresult
