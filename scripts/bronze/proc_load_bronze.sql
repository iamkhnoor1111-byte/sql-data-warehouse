/*
========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accepts any parameters or returns any values.

Use Command to run the procedure:-
  EXEC bronze.load_bronze;
=========================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	BEGIN
	DECLARE @Batch_start_time DATETIME, @Batch_end_time DATETIME;
	SET @Batch_start_time = GETDATE();
		DECLARE @start_time DATETIME, @end_time DATETIME;
		BEGIN TRY

		PRINT '==========================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================================================';


		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------';


	/*
	======================================================================================
	bronze.crm_cust_info
	======================================================================================
	*/

	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- SELECT * FROM bronze.crm_cust_info
	-- SELECT COUNT(*) [Table Count] FROM bronze.crm_cust_info 
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);


	/*
	=========================================================================================
	bronze.crm_prd_info
	=========================================================================================
	*/

	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.crm_prd_info';

	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);

	/*
	=============================================================================================
	bronze.crm_sales_details
	=============================================================================================
	*/
	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);


		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------------';



	/*
	=============================================================================================
	bronze.erp_cust_az12
	=============================================================================================
	*/
	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.erp_cust_az12'
	TRUNCATE TABLE bronze.erp_cust_az12;
	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);


	/*
	===========================================================================================
	bronze.erp_loc_a101
	===========================================================================================
	*/
	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.erp_loc_a101'
	TRUNCATE TABLE bronze.erp_loc_a101;
	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);


	/*
	============================================================================================
	bronze.erp_px_cat_g1v2
	============================================================================================
	*/
	-- TRUNCATE TABLE
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	-- INSERT TABLE
	PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\NOOR KHAN\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
	PRINT '****************************************************************************';

	SET @Batch_end_time = GETDATE();
	PRINT('Whole Batch Duration: ' + CAST(DATEDIFF(second, @Batch_start_time, @Batch_end_time) AS NVARCHAR))

	END TRY

	BEGIN CATCH
		PRINT '========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================================';
	END CATCH
END; 
