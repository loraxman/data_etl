
truncate table etl.w_s_provdrlocnpractspecl;

insert into  etl.w_s_provdrlocnpractspecl
(
ProvdrLocnPractSpeclKey ,
receventtime,
ProvdrLocnPractSpeclPractParStatus ,
ProvdrLocnPractSpeclPrimSpeclFlag,
ProvdrLocnPractSpeclBoardCertCode,
ProvdrLocnPractSpeclBoardCertExpYear,
ProvdrLocnPractSpeclBoardCredCode
ProvdrLocnPractSpeclDisplayFlag
)
select ProvdrLocnPractSpeclKey,
  current_timestamp,
  practice_par_status ,
   prim_spec_ind  ,
  board_certified_code ,
  board_cert_expiration_year ,
  credential_cd ,
  practice_print_ind
from staging.provsrvlocspec a,
vault.h_provdrlocnpractspecl b
where cast (trim(a.pin) as integer) = cast(b.provdrlocnpractspeclProvdrId as integer)
and a.service_location_no = b.ProvdrLocnPractSpeclLocnId
and a.practice_code = ProvdrLocnPractSpeclPractSpeclCode;

select etl.load_s_provdrlocnpractspecl();

select 'PASS';

