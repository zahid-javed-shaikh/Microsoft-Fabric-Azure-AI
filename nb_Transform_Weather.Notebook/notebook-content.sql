-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "52904025-4541-a28c-4803-393657cf19fb",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "52904025-4541-a28c-4803-393657cf19fb",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Clean up if anything was partially created
DROP TABLE IF EXISTS config_cities;
DROP TABLE IF EXISTS fact_weather_hourly;
DROP TABLE IF EXISTS load_log;
DROP PROCEDURE IF EXISTS sp_merge_weather;
DROP PROCEDURE IF EXISTS sp_update_watermark;
DROP PROCEDURE IF EXISTS sp_log_load;
GO

-- ============================================
-- TABLES (note DATETIME2(6) everywhere)
-- ============================================

CREATE TABLE config_cities (
    city_id INT,
    city_name VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    last_watermark DATETIME2(6),
    is_active BIT
);

INSERT INTO config_cities VALUES
(1, 'Mumbai',    19.0760,  72.8777,  '2026-05-20 00:00:00', 1),
(2, 'London',    51.5074,  -0.1278,  '2026-05-20 00:00:00', 1),
(3, 'New York',  40.7128,  -74.0060, '2026-05-20 00:00:00', 1),
(4, 'Tokyo',     35.6762,  139.6503, '2026-05-20 00:00:00', 0);

CREATE TABLE fact_weather_hourly (
    city_id INT,
    city_name VARCHAR(100),
    observation_time DATETIME2(6),
    temperature_c DECIMAL(5,2),
    humidity_pct INT,
    wind_kmh DECIMAL(5,2),
    load_timestamp DATETIME2(6)
);

CREATE TABLE load_log (
    run_id VARCHAR(50),
    city_name VARCHAR(100),
    rows_loaded INT,
    new_watermark DATETIME2(6),
    load_timestamp DATETIME2(6)
);
GO

-- ============================================
-- STORED PROCEDURES
-- ============================================

CREATE PROCEDURE sp_merge_weather
    @CityId INT,
    @CityName VARCHAR(100)
AS
BEGIN
    -- Fabric Warehouse: use DELETE + INSERT instead of MERGE (more reliable)
    DELETE FROM fact_weather_hourly
    WHERE city_id = @CityId
      AND observation_time IN (
          SELECT observation_time 
          FROM lh_weather.dbo.weather_silver
          WHERE city_id = @CityId
      );

    INSERT INTO fact_weather_hourly
        (city_id, city_name, observation_time, temperature_c, humidity_pct, wind_kmh, load_timestamp)
    SELECT 
        city_id, city_name, observation_time, 
        temperature_c, humidity_pct, wind_kmh, 
        GETDATE()
    FROM lh_weather.dbo.weather_silver
    WHERE city_id = @CityId;
END;
GO

CREATE PROCEDURE sp_update_watermark
    @CityId INT,
    @NewWatermark DATETIME2(6)
AS
BEGIN
    UPDATE config_cities
    SET last_watermark = @NewWatermark
    WHERE city_id = @CityId;
END;
GO

CREATE PROCEDURE sp_log_load
    @RunId VARCHAR(50),
    @CityName VARCHAR(100),
    @RowsLoaded INT,
    @NewWatermark DATETIME2(6),
    @LoadTimestamp DATETIME2(6) = NULL
AS
BEGIN
    INSERT INTO load_log VALUES (
        @RunId, 
        @CityName, 
        @RowsLoaded, 
        @NewWatermark, 
        ISNULL(@LoadTimestamp, GETDATE())
    );
END;
GO

-------------------------------------------------------------------

-- 1. MERGE silver → gold (upsert)
CREATE OR ALTER PROCEDURE sp_merge_weather
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
GO

-- 2. Update watermark
CREATE OR ALTER PROCEDURE sp_update_watermark
    @CityId INT,
    @NewWatermark DATETIME2
AS
BEGIN
    UPDATE config_cities
    SET last_watermark = @NewWatermark
    WHERE city_id = @CityId;
END;
GO

-- 3. Log the run
CREATE OR ALTER PROCEDURE sp_log_load
    @RunId VARCHAR(50),
    @CityName VARCHAR(100),
    @RowsLoaded INT,
    @NewWatermark DATETIME2
AS
BEGIN
    INSERT INTO load_log VALUES (@RunId, @CityName, @RowsLoaded, @NewWatermark, GETDATE());
END;
GO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [city_id],
			[city_name],
			[latitude],
			[longitude],
			[last_watermark],
			[is_active]
FROM [DataWorld].[dbo].[config_cities]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
