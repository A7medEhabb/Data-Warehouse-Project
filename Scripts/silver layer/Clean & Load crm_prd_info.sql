/* -- issues in tables

-- check for nulls or Dublicates in PK 
-- should be no nulls
-- no issue
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)  >1 or prd_id is null


-- issue 1 
Extract the first 5 chars of prd_key as a category and replace the '-' with a '_'


-- check for unwanted spaces in prd_nm
-- no issue
select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check for nulls or negative numbers in prd_cst
-- there is null values, we need to replace nulls with zeros
select prd_cost 
from bronze.crm_prd_info
where prd_cost is null or prd_cost <0


-- Data Standardization
-- replace 'M' with 'Mountain', 'R' with 'Road', 'S' with 'Other Sales', 'T' with 'Touring' 
select distinct prd_line from bronze.crm_prd_info

-- issue with date
select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

-- startdate should always be less than end date
and end date should be less than the start date of the next record
and start date shouldn't be null

sol-> end date= start date of the next record - 1 

also we need to remove the time from dates

*/

-- we need to alter the table silver.crm_prd_info before we insert

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




