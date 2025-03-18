-- models/weather_model.sql
SELECT 
    timestamp,
    temperature_2m,
    apparent_temperature,
    wind_speed_10m,
    visibility
FROM 
    weather_data