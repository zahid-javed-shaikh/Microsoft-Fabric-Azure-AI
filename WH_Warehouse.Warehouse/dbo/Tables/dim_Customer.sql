CREATE TABLE [dbo].[dim_Customer] (

	[CustomerKey] int NOT NULL, 
	[CustomerID] int NOT NULL, 
	[CustomerName] varchar(100) NULL, 
	[City] varchar(100) NULL, 
	[EffectiveFrom] date NOT NULL, 
	[EffectiveTo] date NULL, 
	[IsCurrent] bit NOT NULL
);