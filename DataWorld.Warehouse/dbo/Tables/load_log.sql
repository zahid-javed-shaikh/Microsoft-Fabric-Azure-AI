CREATE TABLE [dbo].[load_log] (

	[run_id] varchar(50) NULL, 
	[city_name] varchar(100) NULL, 
	[rows_loaded] int NULL, 
	[new_watermark] datetime2(6) NULL, 
	[load_timestamp] datetime2(6) NULL
);