--delete all that have changed since it is a full replace
delete from provider_hospital_affilliations using staging.provlddelta d
where  d.pin = provider_hospital_affilliations.pin
 and d.service_location_number = provider_hospital_affilliations.service_location_number;


insert into provider_hospital_affilliations
(provider_service_location_id,pin,service_location_number,hospital_pin ,hospital_provider_id,admit_privileges,affil_status_code)
select b.id,
c.pin,b.service_location_number,hospital_pin, c.id ,admit_privileges,affil_status_code
from staging.hospaff a, provider_service_locations b, providers c,staging.provlddelta d
where a.pin = b.pin
and c.pin = a.hospital_pin
and cast(a.service_location_number as integer) = cast(b.service_location_number as integer)
and a.pin = d.pin
and d.change_type in( 'ADD','UPDATE');
--delete types do not get added back

commit;


