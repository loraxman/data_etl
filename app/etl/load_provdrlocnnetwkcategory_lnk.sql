
truncate table etl.w_l_provdrlocnnetwkcategory_provdrlocn;



insert into etl.w_l_provdrlocnnetwkcategory_provdrlocn
(provdrlocnnetwkcategorykey,
provdrlocnkey,
receventtime
  )
 select distinct provdrlocnnetwkcategorykey, provdrlocnkey,
current_timestamp
 from
 vault.h_provdrlocnnetwkcategory a,
 vault.h_provdrlocn b
 where  a.provdrlocnprovdrid  = b.provdrlocnprovdrid
 and a.provdrlocnid = b.provdrlocnid;
 
select etl.load_l_provdrlocnnetwkcategory_provdrlocn();
commit;




insert into etl.w_l_provdrlocnnetwkcategory_netwkcategory
(provdrlocnnetwkcategorykey,
netwkcategorykey,
receventtime
  )
 select distinct provdrlocnnetwkcategorykey, netwkcategorykey,
current_timestamp
 from
 vault.h_provdrlocnnetwkcategory a,
 vault.h_netwkcategory b
 where   a.netwkcategorycode = b.netwkcategorycode;
 
 
select etl.load_l_provdrlocnnetwkcategory_netwkcategory();
commit;



select 'PASS' as status;