

truncate table etl.w_h_provdrlocnnetwkcategory;



insert into etl.w_h_provdrlocnnetwkcategory
(provdrlocnkey,
netwkcategorykey,
provdrlocnprovdrid,
provdrlocnid,
netwkcategorycode 
  )
 select distinct provdrlocnkey, netwkcategorykey,
provdrlocnprovdrid,
provdrlocnid,
netwkcategorycode 
 from staging.srvgrpprovass a,
 vault.h_netwkcategory b,
 vault.h_provdrlocn c
 where a.pin = c.provdrlocnprovdrid
 and a.service_location_no = c.provdrlocnid
 and b.netwkcategorycode = a.category_code ;
 
select etl.load_h_provdrlocnnetwkcategory();
commit;


select 'PASS' as status;