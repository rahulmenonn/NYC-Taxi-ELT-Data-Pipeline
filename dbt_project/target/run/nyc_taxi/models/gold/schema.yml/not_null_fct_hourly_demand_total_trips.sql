
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_trips
from "nyc_taxi"."silver_gold"."fct_hourly_demand"
where total_trips is null



  
  
      
    ) dbt_internal_test