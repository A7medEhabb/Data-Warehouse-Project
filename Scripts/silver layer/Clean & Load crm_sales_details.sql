/*
-- there are issues in sls_order_dt, sls_ship_dt, sls_due_dt there are numbers not dates
also there are zero values in date

select sls_order_dt 
from silver.crm_sales_details
where sls_order_dt <=0

!! issue

select * from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0

*/
IF OBJECT_ID('silver.crm_sales_details','U') is not null
	DROP table silver.crm_sales_details;

create table silver.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt Date,
	sls_ship_dt Date,
	sls_due_dt Date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);


insert into  silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)

select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt =0 or len(sls_order_dt) !=8 then NULL
	Else cast(cast(sls_order_dt as varchar)as Date)
	END sls_order_dt,

	CASE WHEN sls_ship_dt =0 or len(sls_ship_dt) !=8 then NULL
	Else cast(cast(sls_ship_dt as varchar)as Date)
	END sls_order_dt,

	CASE WHEN sls_due_dt =0 or len(sls_due_dt) !=8 then NULL
	Else cast(cast(sls_due_dt as varchar)as Date)
	END sls_due_dt,

	CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
			 THEN sls_quantity * abs(sls_price)
		 ELSE sls_sales
		 END AS sls_sales,

	sls_quantity,
	CASE WHEN sls_price is null or sls_price <=0 
			 THEN sls_sales / NULLIF(sls_quantity,0)
		 ELSE sls_price
		 END AS sls_price

from bronze.crm_sales_details
