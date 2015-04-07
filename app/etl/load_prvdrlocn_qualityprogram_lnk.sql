truncate table etl.w_l_provdrlocn_qualityprogram;



insert into etl.w_l_provdrlocn_qualityprogram
(
provdrlocnkey,
qualityprogramkey,
receventtime
  )
select distinct provdrlocnkey,qualityprogramkey,current_timestamp
from vault.h_provdrlocn a, vault.h_qualityprogram b,
staging.provsrvlocflg c
where c.flag_code = b.qualityprogramcode
and cast(a.provdrlocnprovdrid as int) = cast (trim(c.pin) as int)
and cast(trim(a.provdrlocnid) as integer) = cast(trim(c.service_location_no) as integer);




select etl.load_l_provdrlocn_qualityprogram();
commit;

select 'PASS';
