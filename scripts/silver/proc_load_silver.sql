/*
================================================================================================
Stored Procedure: Load Silver Layer (Bronze => Silver)
================================================================================================
Script Purpose:
  This stored procedure performs ETL (Extract, Transform, Load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncate Silver tables.
  - Insert transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC Silver.load_silver;
================================================================================================
*/

-- EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @Batch_start_time DATETIME, @Batch_end_time DATETIME
    DECLARE @start_time DATETIME, @end_time DATETIME

    SET @Batch_start_time = GETDATE();
    SET @start_time = GETDATE();
    BEGIN TRY

    PRINT '============================================================'
    PRINT 'Loading Sliver Layer'
    PRINT '============================================================'
    
    PRINT '============================================================'
    PRINT 'Loading CRM Tables'
    PRINT '============================================================'
    
    /*
    ===================================================================
        CRM_CUST_INFO
    ===================================================================
    */
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
               cst_id
              ,cst_key
              ,cst_firstname
              ,cst_lastname
              ,cst_material_status
              ,cst_gndr
              ,cst_create_date
        )
        SELECT
               [cst_id]
              ,[cst_key]
              ,TRIM([cst_firstname]) AS cst_firstname
              ,TRIM([cst_lastname]) AS cst_lastname
              ,CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                END cst_material_status
              ,CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    ELSE 'n/a'
               END cst_gndr
              ,[cst_create_date]
        FROM(
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        )t WHERE flag_last = 1

        -- SELECT * FROM silver.crm_cust_info
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
    /*
    ===================================================================
        CRM_PRD_INFO
    ===================================================================
    */

        SET @start_time = GETDATE();
        
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
	        cat_id,
	        prd_key,
	        prd_nm,
	        prd_cost,
	        prd_line,
	        prd_start_date,
	        prd_end_dt
        )

        SELECT [prd_id]
              ,REPLACE(SUBSTRING([prd_key],1,5),'-','_') AS cat_id  -- Extract category ID
              ,SUBSTRING([prd_key], 7, LEN(prd_key)) AS prd_key     -- Extract product key
              ,[prd_nm]
              ,COALESCE([prd_cost], 0) AS prd_cost
              ,CASE TRIM(UPPER(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
               END AS prd_line     -- Map product line codes to descriptive values 
              ,CAST([prd_start_date] AS DATE)
              ,CAST(
                    LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) - 1 AS DATE) 
                    AS prd_end_dt   -- Calculate end date as one day before the next start date 
          FROM bronze.crm_prd_info

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
    /*
    ========================================================================
        CRM_SALES_DETAILS
    ========================================================================
    */

        SET @start_time = GETDATE();

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
               [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,[sls_order_dt]
              ,[sls_ship_dt]
              ,[sls_due_dt]
              ,[sls_sales]
              ,[sls_quantity]
              ,[sls_price]
        )
        SELECT [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) != 8 THEN NULL
                    WHEN LEN([sls_order_dt]) = 8 THEN CAST(CAST([sls_order_dt] AS CHAR(8)) AS DATE)  
              END AS [sls_order_dt]
              ,CAST(CAST([sls_ship_dt] AS CHAR(8)) AS DATE) AS sls_ship_dt
              ,CAST(CAST([sls_due_dt] AS CHAR(8)) AS DATE) AS sls_due_dt
              ,CASE WHEN [sls_sales] IS NULL 
                      OR [sls_sales] <= 0 
                      OR [sls_sales] != sls_quantity * ABS(sls_price) 
                      THEN sls_quantity * ABS(sls_price)
                    ELSE [sls_sales]
              END AS sls_sales
              ,[sls_quantity]
              ,CASE WHEN [sls_price] IS NULL OR [sls_price] <= 0 
                    THEN [sls_sales] / NULLIF([sls_quantity],0)
                    ELSE [sls_price]
              END AS sls_price
          FROM bronze.crm_sales_details

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';

    PRINT '============================================================'
    PRINT 'Loading ERP Tables'
    PRINT '============================================================'
    
    /*
    ======================================================================
        ERP_CUST_AZ12
    ======================================================================
    */
        SET @start_time = GETDATE();

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            [cid]
           ,[bdate]
           ,[gen]
        )
        SELECT
               CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
               END AS cid
              ,CASE WHEN [bdate] > GETDATE()  THEN NULL
                    ELSE bdate
               END AS bdate
              ,CASE WHEN UPPER(TRIM([gen])) IN ('F', 'FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM([gen])) IN ('M', 'MALE') THEN 'Male'
                    ELSE 'n/a'
               END AS gen
          FROM [DataWarehouse].[bronze].[erp_cust_az12]

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
    /*
    =========================================================================
        ERP_LOC_A101
    =========================================================================
    */
        SET @start_time = GETDATE();

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
	        cid,
	        cntry
        )
        SELECT 
	        REPLACE(cid,'-', '') AS cid,
	        CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		         WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
		         WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'n/a'
		         ELSE cntry
	        END AS cntry
        FROM bronze.erp_loc_a101

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
    /*
    =========================================================================
        ERP_PX_CAT_G1V2
    =========================================================================
    */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(
	        id,
	        cat,
	        subcat,
	        maintenance
        )
        SELECT 
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2
        
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';

        SET @Batch_end_time = GETDATE();
        PRINT 'Whole Batch Duration: ' + CAST(DATEDIFF(second, @Batch_start_time, @Batch_end_time) AS NVARCHAR)+ ' seconds';
    END TRY

    BEGIN CATCH
        PRINT 'Error Occured during Loading Bronze Layer';
        PRINT 'Error Message: ' + ERROR_Message();
        PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END;
