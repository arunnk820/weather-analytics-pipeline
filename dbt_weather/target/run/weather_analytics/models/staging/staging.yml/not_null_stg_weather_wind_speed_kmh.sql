
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select wind_speed_kmh
from WEATHER_DB.ANALYTICS.stg_weather
where wind_speed_kmh is null



  
  
      
    ) dbt_internal_test