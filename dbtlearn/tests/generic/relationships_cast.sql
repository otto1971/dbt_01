{% test relationships_cast(model, column_name, to, field) %}

with
target_table as (
    select cast({{column_name}} as char(40)) as target_column
    from {{model}} 
),
source_table as (
    select cast({{field}} as char(40)) as source_column
    from {{ ref('to')}} 
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
select * from finalresult

{% endtest %}