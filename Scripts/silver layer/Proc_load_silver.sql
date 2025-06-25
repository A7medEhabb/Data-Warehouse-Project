/*
The stored Procedure used to load data from the source Bronze layer into the Silver Layer, calculate the time required to load each table, and
Calculate the overall time required for the whole process.

This SP doesn't accept any parameters or return any value.

Usage Example:
	EXEC bronze.load_bornze;

*/

create or alter procedure silver.load_silver as 
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time datetime, @batch_end_time datetime;
	Begin TRY
		set @batch_start_time=GETDATE();
		print '----------------------------------------------------------------------';
		print 'Loading Silver layer';
		print '----------------------------------------------------------------------';

		print '##################################################################'
		print 'Loading CRM Tables';
		print '##################################################################'

		
		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_cust_info ';
		Truncate table silver.crm_cust_info;
		print '>> inserting Data into: silver.crm_cust_info'
		Insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select cst_id, cst_key, TRIM(cst_firstname) as cst_firstname , TRIM(cst_lastname) as cst_lastname, CASE WHEN upper(TRIM(cst_marital_status))='S' then 'Single'
			 WHEN upper(TRIM(cst_gndr))='M' then 'Married'
			 else 'N/A'
		END cst_marital_status,
		CASE WHEN upper(TRIM(cst_gndr))='F' then 'Female'
			 WHEN upper(TRIM(cst_gndr))='M' then 'Male'
			 else 'N/A'
		END cst_gndr,
		cst_create_date
		from (

			select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_last =1 
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		-----------------------------------------------------------------------------------

		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
		print '>> inserting Data into: silver.crm_prd_info'

		IF OBJECT_ID('silver.crm_prd_info','U') is not null
			DROP table silver.crm_prd_info;

		create table silver.crm_prd_info (
			prd_id int,
			cat_id nvarchar(50),
			prd_key nvarchar(50),
			prd_nm nvarchar(50),
			prd_cost int,
			prd_line nvarchar(50),
			prd_start_dt date,
			prd_end_dt date,
			dwh_create_date datetime2 default getdate()
		);

		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		select 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7, len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			CASE WHEN upper(TRIM(prd_line))='M' then 'Mountain'
				 WHEN upper(TRIM(prd_line))='R' then 'Road'
				 WHEN upper(TRIM(prd_line))='S' then 'Other Sales'
				 WHEN upper(TRIM(prd_line))='T' then 'Touring'
				 ELSE 'N/A'
			END prd_line,
			cast(prd_start_dt as Date) as prd_start_dt,
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as Date)as prd_end_dt
		from bronze.crm_prd_info
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';

		-------------------------------------------------------------------------------------------------------------
		
		set @start_time=GETDATE();
		print '>> Truncating table: silver.crm_sales_details';
		Truncate table silver.crm_sales_details;
		print '>> inserting Data into: silver.crm_sales_details'

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
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		print '##################################################################'
		print 'Loading ERP Tables';
		print '##################################################################'

		------------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12;
		print '>> inserting Data into: silver.erp_cust_az12'


		insert into silver.erp_cust_az12 (
	
			CID,
			Bdate,
			GEN
		)


		select
			CASE WHEN CID like 'NAS%' THEN SUBSTRING(CID,4,len(CID))
			ELSE CID   -- remove first 3 char so we can make a relation between this table and crm_cust_info with CID column
			END as CID,

			CASE WHEN BDATE > GETDATE() THEN NULl
			ELSE BDATE   -- set future birthdays to null
			END as BDATE,
			CASE WHEN UPPER(TRIM(GEN)) in ('F','Female') THEN 'Female'
				 WHEN UPPER(TRIM(GEN)) in ('M','Male') THEN 'Male'
				 ELSE 'N/A'
				 END as GEN
		from bronze.erp_cust_az12
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		------------------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101;
		print '>> inserting Data into: silver.erp_loc_a101'

		insert into silver.erp_loc_a101(
			CID,
			CNTRY
		)

		select 
			replace(cid,'-','') as CID,
			Case When trim(cntry)='DE' then 'Germany'
				 When trim(cntry) in ('USA','US','United States') then 'United States'
				 When trim(cntry) ='' or cntry is null then 'N/A'
				 else CNTRY
				 end as CNTRY

		from bronze.erp_loc_a101
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';


		-------------------------------------------------------------------------------------
		
		set @start_time=GETDATE();
		print '>> Truncating table: silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2;
		print '>> inserting Data into: silver.erp_px_cat_g1v2'

		insert into silver.erp_px_cat_g1v2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		select  
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE

		from bronze.erp_px_cat_g1v2
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		set @batch_end_time= GETDATE();

		print'Loading Bronze Layer is completed ';
		print 'total load Duration' + cast(Datediff(second, @batch_start_time , @batch_end_time) as nvarchar) + ' Seconds';
	END TRY
	BEGIN catch
	Print 'Error occoured During loading Silver layer'
	print 'Error Message' + Error_message();
	END Catch
END
