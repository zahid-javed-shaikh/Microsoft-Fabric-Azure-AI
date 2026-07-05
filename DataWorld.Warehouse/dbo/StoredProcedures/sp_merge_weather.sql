-------------------------------------------------------------------

-- 1. MERGE silver → gold (upsert)
CREATE   PROCEDURE sp_merge_weather
    @CityId INT,
    @CityName VARCHAR(100)
AS
BEGIN
    MERGE fact_weather_hourly AS target
    USING (
        SELECT * FROM lh_weather.dbo.weather_silver
        WHERE city_id = @CityId
    ) AS source
    ON target.city_id = source.city_id 
       AND target.observation_time = source.observation_time
    WHEN MATCHED THEN UPDATE SET
        temperature_c = source.temperature_c,
        humidity_pct = source.humidity_pct,
        wind_kmh = source.wind_kmh,
        load_timestamp = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (city_id, city_name, observation_time, temperature_c, humidity_pct, wind_kmh, load_timestamp)
        VALUES (source.city_id, source.city_name, source.observation_time,
                source.temperature_c, source.humidity_pct, source.wind_kmh, GETDATE());
END;