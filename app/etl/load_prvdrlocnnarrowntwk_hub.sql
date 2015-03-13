truncate table etl.w_h_provdrlocnnarrownetwk;

insert into etl.w_h_provdrlocnnarrownetwk
(ProvdrLocnProvdrId,
  ProvdrLocnId ,
  NarrowNetwkCode
  )
select distinct ProvdrLocnProvdrId,
ProvdrLocnId  ,
NarrowNetwkCode
from vault.h_provdrlocn a,
vault.h_narrownetwk b,
staging.provntwkloc c
where cast(a.provdrlocnprovdrid as integer) = cast(c.pin as integer)
and cast(a.provdrlocnid as integer) = cast(c.addr_no as integer)
and cast(b.narrownetwkcode as integer) = cast(c.base_net_id_no  as integer);


select etl.load_h_provdrlocnnarrownetwk();
commit;




select 'PASS' as status;