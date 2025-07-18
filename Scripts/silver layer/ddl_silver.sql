/*

This Script contains the DDL statements in the silver Layer 


*/



IF OBJECT_ID('silver.crm_cust_info','U') is not null
	DROP table silver.crm_cust_info;

Create table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.crm_prd_info','U') is not null
	DROP table silver.crm_prd_info;

create table silver.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.crm_sales_details','U') is not null
	DROP table silver.crm_sales_details;

create table silver.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.erp_cust_az12','U') is not null
	DROP table silver.erp_cust_az12;

create table silver.erp_cust_az12(
	CID nvarchar(50),
	BDATE Date,
	GEN nvarchar(50),
	dwh_create_date datetime2 default getdate()

);
IF OBJECT_ID('silver.erp_loc_a101','U') is not null
	DROP table silver.erp_loc_a101;

create table silver.erp_loc_a101(
	CID nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date datetime2 default getdate()

);

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') is not null
	DROP table silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date datetime2 default getdate()

);
