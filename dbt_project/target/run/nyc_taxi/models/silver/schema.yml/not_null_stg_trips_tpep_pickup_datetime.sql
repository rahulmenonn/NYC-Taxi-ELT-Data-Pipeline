
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tpep_pickup_datetime
from "nyc_taxi"."silver_silver"."stg_trips"
where tpep_pickup_datetime is null



  
  
      
    ) dbt_internal_test