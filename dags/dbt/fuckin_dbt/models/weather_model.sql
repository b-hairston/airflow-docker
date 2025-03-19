-- models/weather_model.sql
SELECT
    observed_at,
    (temperature* 9/5) + 32 AS temperature_fahrenheit,
    (apparent_temperature * 9/5) + 32 AS apparent_temperature_fahrenheit,
    wind_speed * 0.621371 AS wind_speed_mph,
    visibility * 3.28084 AS visibility_feet
FROM 
    weather_data