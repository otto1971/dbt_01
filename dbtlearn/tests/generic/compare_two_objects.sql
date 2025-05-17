{% test compare_two_objects(target_model, source_model, target_column, source_column) %}

with
target_table as (
    select cast({{target_column}} as char(40)) as target_column
    from {{target_model}} 
),
source_table as (
    select cast({{source_column}} as char(40)) as source_column
    from {{source_model}} 
),
finalresult as
(
select target_column as result_data
from target_table 
where target_column not in 
    (
    select source_column from source_table
    )
)
select count(result_data) from finalresult

{% endtest %}