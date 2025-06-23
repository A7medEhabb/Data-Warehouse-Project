/*

This scripts creates a database named 'DataWarehouse' after checking if not already existed. Also, the script
creates three schemas representing our DataWarehouse layers (Bronze, Silver, Gold)


*/



IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END

GO

Use DataWarehouse;

GO

create schema bronze;
Go
create schema silver;
Go
create schema gold;


