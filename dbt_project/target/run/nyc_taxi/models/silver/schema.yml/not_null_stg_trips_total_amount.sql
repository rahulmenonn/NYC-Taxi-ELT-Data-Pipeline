
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from "nyc_taxi"."silver_silver"."stg_trips"
where total_amount is null



  
  
      
    ) dbt_internal_test