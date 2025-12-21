CREATE TABLE [raw].[products] (

	[product_id] int NOT NULL, 
	[product_name] varchar(100) NOT NULL, 
	[category] varchar(50) NULL, 
	[price] decimal(10,2) NULL, 
	[date_added] date NULL
);