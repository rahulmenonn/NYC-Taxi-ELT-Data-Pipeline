
    
    

with all_values as (

    select
        payment_type_name as value_field,
        count(*) as n_records

    from "nyc_taxi"."silver_silver"."stg_trips"
    group by payment_type_name

)

select *
from all_values
where value_field not in (
    'Credit Card','Cash','No Charge','Dispute','Unknown','Other'
)


