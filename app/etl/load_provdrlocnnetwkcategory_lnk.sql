
truncate table etl.w_l_provdrlocnnetwkcategory_provdrlocn;



insert into etl.w_l_provdrlocnnetwkcategory_provdrlocn;
(provdrlocnnetwkcategorykey,
provdrlocnkey,
receventtime
  )
 select distinct provdrlocnnetwkcategorykey, provdrlocnkey,
current_timestamp
 from
 vault.h_provdrlocnnetwkcategory a,
 vault.h_provdrlocn b
 where a.provdrlocnkey = b.provdrlocnkey;
 
select etl.load_l_provdrlocnnetwkcategory_provdrlocn();
commit;


select 'PASS' as status;