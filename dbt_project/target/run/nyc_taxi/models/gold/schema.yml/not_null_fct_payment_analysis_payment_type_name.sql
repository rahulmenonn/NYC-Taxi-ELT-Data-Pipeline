
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type_name
from "nyc_taxi"."silver_gold"."fct_payment_analysis"
where payment_type_name is null



  
  
      
    ) dbt_internal_test