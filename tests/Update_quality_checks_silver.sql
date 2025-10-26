/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
  - NULLs or dupicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver layer.
  - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
/*
================================================================================
Checking 'silver.crm_cust_info'
================================================================================
*/
-- Check for NULLs or Duplicates in Primary key
  -- Expectations: No resuls
  SELECT * 
  FROM silver.crm_cust_info
  WHERE cst_id IS NULL OR cst_id < 0
  OR cst_key != TRIM(cst_key) OR cst_key = ''
  OR cst_key IS NULL

  -- Data Standardization & Consistency
  SELECT DISTINCT prd_line
  FROM silver.crm_prd_info

  -- Checking for Invalid Date Orders
  SELECT * FROM silver.crm_prd_info
  WHERE prd_end_dt < prd_start_date

======================================================================================================
Checking 'silver.erp_px_cat_g1v2'
======================================================================================================
-- Checking for unwanted Spaces
SELECT 
id
FROM silver.erp_px_cat_g1v2
WHERE id != TRIM(id)

-- Date Standardization & Consistency
SELECT DISTINCT
cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT prd_nm FROM silver.crm_prd_info
ORDER BY prd_nm

SELECT DISTINCT
subcat
FROM silver.erp_px_cat_g1v2
WHERE subcat NOT IN (SELECT prd_nm FROM silver.crm_prd_info)
ORDER BY subcat

SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2
ORDER BY maintenance

=======================================================================================================
Checking crm_sales_details
=======================================================================================================

-- Checking silver table

SELECT
*
FROM silver.crm_sales_details

-- checking date errors

-- sls_order_dt

SELECT 
NULLIF(sls_order_dt,0) 
FROM silver.crm_sales_details
WHERE sls_order_dt = 0

SELECT 
NULLIF(sls_order_dt,0) 
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101

SELECT 
NULLIF(sls_order_dt,0) 
FROM silver.crm_sales_details
WHERE sls_order_dt > 20500101

SELECT 
NULLIF(sls_order_dt,0) 
FROM silver.crm_sales_details
WHERE sls_order_dt < 19000101


SELECT
sls_order_dt,
LEN(sls_order_dt)
FROM silver.crm_sales_details

-- sls_ship_dt

SELECT
sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8


SELECT 
sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt < 0 OR sls_ship_dt IS NULL

SELECT 
NULLIF(sls_ship_dt,0) 
FROM silver.crm_sales_details
WHERE sls_ship_dt > 20500101

SELECT 
NULLIF(sls_ship_dt,0) 
FROM silver.crm_sales_details
WHERE sls_ship_dt < 19000101




-- sls_due_date

SELECT
sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 OR sls_due_dt IS NULL

SELECT
sls_due_dt
FROM silver.crm_sales_details
WHERE LEN(sls_due_dt) != 8 

SELECT 
NULLIF(sls_due_dt,0) 
FROM silver.crm_sales_details
WHERE sls_due_dt > 20500101

SELECT 
NULLIF(sls_due_dt,0) 
FROM silver.crm_sales_details
WHERE sls_due_dt < 19000101

-- Checking sls_price
SELECT
sls_price,
sls_quantity
FROM silver.crm_sales_details
WHERE sls_price IS NULL OR sls_price <= 0

-- Checking sls_quantity
SELECT
sls_quantity
FROM silver.crm_sales_details
WHERE sls_quantity < 0 OR sls_quantity IS NULL

-- Checking Sales, Quantity and price relation

SELECT 
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

======================================================================================================
Checking silver.erp_cust_az12
======================================================================================================
-- Checking blank spaces in cid
SELECT
cid
FROM silver.erp_cust_az12
WHERE cid != TRIM(cid)



SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate IS NULL

-- Data Standardization & Consistency

SELECT DISTINCT
    gen,
    CASE WHEN UPPER(TRIM([gen])) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM([gen])) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a'
    END AS gen
FROM silver.erp_cust_az12

SELECT DISTINCT gen
FROM silver.erp_cust_az12
WHERE gen IN (SELECT gen FROM silver.crm_cust_info)

========================================================================================================
Checking silver.erp_loc_a101
========================================================================================================
-- Cleaning Data Data Standardization & Consistency
SELECT 
	LEN(SUBSTRING(cid, 7, LEN(cid))) AS cid
FROM silver.erp_loc_a101

SELECT 
	cntry
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry)

SELECT DISTINCT
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) = 'US' THEN 'United States'
		 WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'n/a'
		 ELSE cntry -- Normalize and Handle missing or blank country codes
	END AS cntry
FROM silver.erp_loc_a101
