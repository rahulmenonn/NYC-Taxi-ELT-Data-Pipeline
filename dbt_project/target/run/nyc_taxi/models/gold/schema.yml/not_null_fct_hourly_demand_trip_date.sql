
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_date
from "nyc_taxi"."silver_gold"."fct_hourly_demand"
where trip_date is null



  
  
      
    ) dbt_internal_test