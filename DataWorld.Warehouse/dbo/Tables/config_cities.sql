CREATE TABLE [dbo].[config_cities] (

	[city_id] int NULL, 
	[city_name] varchar(100) NULL, 
	[latitude] decimal(9,6) NULL, 
	[longitude] decimal(9,6) NULL, 
	[last_watermark] datetime2(6) NULL, 
	[is_active] bit NULL
);