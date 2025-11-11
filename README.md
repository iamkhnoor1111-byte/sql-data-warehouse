# sql-data-warehouse
Building a modern data warehouse with SQL Server, including ETL processes, data modeling, and analytics.


Data Warehouse Project (Bronze, Silver, Gold Layers)
Overview

This project follows the Medallion Architecture — a layered approach to building a modern data warehouse.
The three main layers are Bronze, Silver, and Gold, each with a clear purpose and data quality level.

1. Bronze Layer – Raw Data

The Bronze layer stores raw data exactly as it comes from the source systems.
It acts as the landing zone for all incoming data — no cleaning or transformation happens here.

Key Points:

Data is collected from different sources (CSV, APIs, databases, etc.).

Stored in its original format.

Helps preserve data history for auditing or recovery.

Example Files:

bronze.crm_customers_raw.csv

bronze.erp_sales_raw.csv

bronze.inventory_raw.csv

Purpose: Keep a full copy of the raw data before any transformation.

2. Silver Layer – Cleaned and Standardized Data

The Silver layer processes data from the Bronze layer to make it clean, consistent, and ready for analysis.
It handles data quality tasks such as:

Removing duplicates

Fixing null or invalid values

Standardizing data types and formats

Joining data from multiple sources

Example Tables:

silver.crm_customer_info

silver.erp_product_info

silver.sales_fact

Purpose: Prepare clean, reliable data that can be used confidently for business reporting or analysis.

3. Gold Layer – Business-Ready Data

The Gold layer contains data that is fully aggregated, modeled, and ready for reporting or dashboards.
It focuses on business logic and metrics, such as sales performance, customer insights, or KPIs.

Example Tables/Views:

gold.dim_customers – Customer dimension table

gold.dim_products – Product dimension table

gold.fact_sales – Fact table for sales analysis

Common Analysis:

Ranking (top products/customers)

Time-based trends (month-over-month growth)

Cumulative and performance analysis

Reports for Power BI or Tableau

Purpose: Deliver final, business-friendly data for decision-making and analytics.

4. Tools and Technologies

SQL Server / Azure SQL Database – For storing and transforming data

SSMS / Azure Data Studio – For running SQL scripts

5. Project Flow

Load raw data into Bronze tables.

Clean and transform data into Silver tables.

Aggregate and model final tables in Gold.

Connect Gold tables to Power BI for visualization.

6. Benefits of the Layered Approach

Easier debugging and error tracking.

Clear data lineage (you can trace every record).

Reusable transformations and scalable design.

Reliable foundation for analytics and reporting.
