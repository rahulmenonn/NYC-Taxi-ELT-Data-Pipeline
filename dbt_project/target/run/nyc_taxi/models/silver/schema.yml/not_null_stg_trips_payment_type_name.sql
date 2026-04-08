
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type_name
from "nyc_taxi"."silver_silver"."stg_trips"
where payment_type_name is null



  
  
      
    ) dbt_internal_test