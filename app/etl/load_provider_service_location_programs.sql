
--delete all that have changed since it is a full replace
delete from provider_service_location_programs  using staging.provlddelta d
where  d.pin = provider_service_location_programs.pin
 and d.service_location_number = provider_service_location_programs.service_location_number;


insert into provider_service_location_programs
(provider_service_location_id,pin,service_location_no,flag_code,flag_rollup,flag_description,procedure_type)
select b.id,
b.pin,b.service_location_number,trim(flag_code),trim(flag_rollup),trim(flag_description),trim(procedure_type)
from staging.provsrvlocflg a, provider_service_locations b, staging.provlddelta d
where a.pin = b.pin
and cast(a.service_location_no as integer) = cast(b.service_location_number as integer)
and a.pin = d.pin
 and d.service_location_number = b.service_location_number
and d.change_type in( 'ADD','UPDATE');

commit;
