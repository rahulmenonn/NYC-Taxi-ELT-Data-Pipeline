
    
    

select
    trip_date as unique_field,
    count(*) as n_records

from "nyc_taxi"."silver_gold"."fct_daily_summary"
where trip_date is not null
group by trip_date
having count(*) > 1


