
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select time_period
from "nyc_taxi"."silver_gold"."fct_hourly_demand"
where time_period is null



  
  
      
    ) dbt_internal_test