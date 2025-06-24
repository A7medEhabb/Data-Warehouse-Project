/*
-- check for nulls or Dublicates in PK 
-- should be no nulls

-- issue 1
select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)  >1 or cst_id is null

--check unwanted spaces in string values
-- issue 2
select cst_firstname 
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)
select cst_lastname 
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select cst_gndr 
from bronze.crm_cust_info
where cst_gndr  != TRIM(cst_gndr )

-- Data Standarization & consistency in cst_marital_status,  cst_gndr columns
-- no issue but we can replace the abbriviation into full word.
-- issue 3
select Distinct cst_gndr
from bronze.crm_cust_info

select Distinct cst_marital_status
from bronze.crm_cust_info

----------------------------------------------------------------------------------------------------------------------------------


*/






Insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)


-- solve issue 1
/*
select * from (
select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t
where flag_last =1 

*/
-- solve issue 2, 3

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





