

--delete all that have changed since it is a full replace
delete from provider_service_location_programs  using staging.provlddelta d,provider_service_locations b
where  d.pin = b.pin
 and d.service_location_number = b.service_location_number
and b.id = provider_service_location_programs.provider_service_location_id;


insert into provider_service_location_programs
(provider_service_location_id,pin,service_location_no,flag_code,flag_rollup,flag_description,procedure_type)
select distinct b.id,
b.pin,b.service_location_number,trim(flag_code),trim(flag_rollup),trim(flag_description),trim(procedure_type)
from staging.provsrvlocflg a, provider_service_locations b
where a.pin = b.pin
and cast(a.service_location_no as integer) = cast(b.service_location_number as integer);

commit;
