/*
Thie Stored Procedure used to load data from source files into Bronze Layer, calculate the time required to load each table, and
calculate the overall time required for the whole process.

This SP doesn't accept any parameters or return any value.

Usage Example:
	EXEC bronze.load_bornze;

*/


Create or alter Proc bronze.load_bornze AS
BEGIN
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time datetime, @batch_end_time datetime;
	Begin TRY
		set @batch_start_time=GETDATE();
		print '----------------------------------------------------------------------';
		print 'Loading bronze layer';
		print '----------------------------------------------------------------------';

		print '##################################################################'
		print 'Loading CRM Tables';
		print '##################################################################'


		set @start_time=GETDATE();
		print '>> Truncating table: bronze.crm_cust_info'
		Truncate table bronze.crm_cust_info;
	
		print '>> Inserting Data Into: bronze.crm_cust_info'
		Bulk Insert bronze.crm_cust_info
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator =',',
			Tablock -- Lock table during the insert process
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		

		set @start_time=GETDATE();
		print '>> Truncating table: bronze.crm_prd_info'
		Truncate table bronze.crm_prd_info;
		print '>> Inserting Data Into: bronze.crm_prd_info'
		Bulk Insert bronze.crm_prd_info
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';

	
		set @start_time=GETDATE();
		print '>> Truncating table: bronze.crm_sales_details'
		Truncate table bronze.crm_sales_details;
		print '>> Inserting Data Into: bronze.crm_sales_details'
		Bulk Insert bronze.crm_sales_details
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		print '##################################################################'
		print 'Loading ERP Tables';
		print '##################################################################'


		set @start_time=GETDATE();
		print '>> Truncating table: bronze.erp_cust_az12'
		Truncate table bronze.erp_cust_az12;
		print '>> Inserting Data Into: bronze.erp_cust_az12'
		Bulk Insert bronze.erp_cust_az12
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		set @start_time=GETDATE();
		print '>> Truncating table: bronze.erp_loc_a101'
		Truncate table bronze.erp_loc_a101;
		print '>> Inserting Data Into: bronze.erp_loc_a101'
		Bulk Insert bronze.erp_loc_a101
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';

		set @start_time=GETDATE();
		print '>> Truncating table: bronze.erp_px_cat_g1v2'
		Truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		Bulk Insert bronze.erp_px_cat_g1v2
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
		set @end_time=GETDATE();
		print'>> Load Duration: '+ cast( Datediff(second, @start_time,@end_time)as nvarchar) +' Seconds';
		print '===========================================================================================';
		
		set @batch_end_time= GETDATE();

		print'Loading Bronze Layer is completed ';
		print 'total load Duration' + cast(Datediff(second, @batch_start_time , @batch_end_time) as nvarchar) + ' Seconds';
		ENd TRY
		Begin Catch
			Print 'Error occoured During loading Bronze layer'
			print 'Error Message' + Error_message();
		End Catch
END
