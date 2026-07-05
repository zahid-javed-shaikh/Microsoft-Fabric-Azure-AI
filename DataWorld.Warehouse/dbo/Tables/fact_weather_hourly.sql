CREATE TABLE [dbo].[fact_weather_hourly] (

	[city_id] int NULL, 
	[city_name] varchar(100) NULL, 
	[observation_time] datetime2(6) NULL, 
	[temperature_c] decimal(5,2) NULL, 
	[humidity_pct] int NULL, 
	[wind_kmh] decimal(5,2) NULL, 
	[load_timestamp] datetime2(6) NULL
);