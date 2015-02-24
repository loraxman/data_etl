

truncate table w_l_provdrlocnpractspecl_provdrlocn;

insert into w_l_provdrlocnpractspecl_provdrlocn
(provdrlocnpractspeclkey,
  provdrlocnkey ,
  recefftime 
  )
 select provdrlocnpractspeclkey, provdrlocnkey,current_timestamp
 from h_provdrlocnpractspecl a,
 h_provdrlocn b,
 provsrvlocspec c
 where c.pin = b.provdrid
 and c.service_location_no = b.provdrlocnid
 and a.provdrlocnpractspecllocnid = b.provdrlocnid
 and a.provdrlocnpractspeclprovdrid = c.pin
 and a.provdrlocnpractspeclpractspeclcode = c.practice_code;
select load_l_provdrlocnpractspecl_provdrlocn();
commit;


truncate table w_l_provdrlocnpractspecl_practspecl;

insert into w_l_provdrlocnpractspecl_practspecl
(provdrlocnpractspeclkey,
  practspeclkey ,
  recefftime 
  )
 select distinct provdrlocnpractspeclkey, practspeclkey,current_timestamp
 from h_provdrlocnpractspecl a,
 h_practspecl b,
 provsrvlocspec c
 where b.practspeclcode = a.provdrlocnpractspeclpractspeclcode
 and a.provdrlocnpractspeclprovdrid = c.pin
 and a.provdrlocnpractspeclpractspeclcode = c.practice_code;
select load_l_provdrlocnpractspecl_provdrlocn();
commit;
