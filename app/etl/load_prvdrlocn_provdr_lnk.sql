
truncate table w_l_provdrlocn_provdr;

insert into w_l_provdrlocnpractspecl_provdrlocn
(provdrlocnkey,
  provdrkey ,
  recefftime 
  )
 select provdrlocnkey, provdrkey,current_timestamp
 from h_provdrlocn a,
 h_provdr b 
 where 
 a.provdrlocnprovdrid = b.provdrid;
select load_l_provdrlocn_provdr();
commit;

