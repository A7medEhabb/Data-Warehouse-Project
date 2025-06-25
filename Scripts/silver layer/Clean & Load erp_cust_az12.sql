/*
CID used to connect with crm_cust_info table but it has 'NAS' in the first so we need to remove it.

-- there are BDATE that out of range (in the future)
select BDATE from bronze.erp_cust_az12 where BDATE > getdate()

-- Issue in GEN column ('F','M','Male','Female')
select distinct GEN from bronze.erp_cust_az12
*/


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





