
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ingested_at
from WEATHER_DB.ANALYTICS.fct_weather_alerts
where ingested_at is null



  
  
      
    ) dbt_internal_test