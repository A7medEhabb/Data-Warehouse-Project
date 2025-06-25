/*
-- issue 1 -> CID column has '-' in it while cst_key which we will use in relation doesn't have '-' in it so we need to fix this.
-- issue 2 
check -> select distinct cntry from bronze.erp_loc_a101
*/
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
