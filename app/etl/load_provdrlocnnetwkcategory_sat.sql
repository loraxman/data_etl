
truncate table etl.w_s_provdrlocnnetwkcategory;



insert into etl.w_s_provdrlocnnetwkcategory
(provdrlocnnetwkcategorykey,
provdrlocnnetwkcategorycurrenttier ,
receventtime
  )
  
 select distinct provdrlocnnetwkcategorykey,current_tier,

current_timestamp
 from staging.srvgrpprovass a,
 vault.h_netwkcategory b,
 vault.h_provdrlocn c
 where a.pin = c.provdrlocnprovdrid
 and a.service_location_no = c.provdrlocnid
 and b.netwkcategorycode = a.category_code ;
 
select etl.load_s_provdrlocnnetwkcategory();
commit;


select 'PASS' as status;