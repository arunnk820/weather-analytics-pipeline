
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observation_date
from WEATHER_DB.ANALYTICS.fct_weather_daily
where observation_date is null



  
  
      
    ) dbt_internal_test