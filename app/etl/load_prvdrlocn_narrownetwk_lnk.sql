truncate table etl.w_l_provdrlocnnarrownetwk_provdrlocn;

insert into etl.w_l_provdrlocnnarrownetwk_provdrlocn
(provdrlocnnarrownetwkkey,
  provdrlocnkey,
  receventtime 
  )
select  provdrlocnnarrownetwkkey,provdrlocnkey, current_timestamp
from vault.h_provdrlocn a,
vault.h_provdrlocnnarrownetwk b
where a.provdrlocnprovdrid = b.provdrlocnprovdrid
and a.provdrlocnid = b.provdrlocnid;

select etl.load_l_provdrlocnnarrownetwk_provdrlocn();
commit;


truncate table etl.w_l_provdrlocnnarrownetwk_narrownetwk;

insert into etl.w_l_provdrlocnnarrownetwk_narrownetwk
(provdrlocnnarrownetwkkey,
  narrownetwkkey,
  receventtime 
  )
  select  provdrlocnnarrownetwkkey,narrownetwkkey, current_timestamp
from vault.h_narrownetwk a,
vault.h_provdrlocnnarrownetwk b
where a.narrownetwkcode = b.narrownetwkcode;

select etl.load_l_provdrlocnnarrownetwk_narrownetwk();
commit;


select 'PASS' as status;