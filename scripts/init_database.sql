/*
==========================================================================================
Create Database and Schemas
==========================================================================================
Script Purpose:
  The script creates a new database name 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreate. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver', 'gold'

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanentlt deleted. Proceeed with caution
  and ensure you have proper backups before running this script
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWatehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DateWarehouse
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;









