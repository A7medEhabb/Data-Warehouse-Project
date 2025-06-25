/*
	 This table has High Data Quality
	 no issues found!
*/
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

