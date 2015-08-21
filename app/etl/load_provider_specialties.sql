
--delete all that have changed since it is a full replace
delete from provider_service_location_specialties  using staging.provlddelta d, provider_service_locations b
where  d.pin = b.pin
 and d.service_location_number = b.service_location_number
and b.id = provider_service_location_specialties.provider_service_location_id;



insert into provider_service_location_specialties(
provider_service_location_id,pin,service_location_no,practice_type,practice_code,
practice_description,rollup_code,rollup_description,prim_spec_ind,practice_par_status,practice_print_ind,board_certified_code,
board_cert_expiration_year,credential_cd,age_band_min,age_band_1024,age_band_description)

select b.id,b.pin,service_location_no,practice_type,practice_code,
practice_description,rollup_code,rollup_description,prim_spec_ind,practice_par_status,practice_print_ind,board_certified_code,
board_cert_expiration_year,credential_cd,age_band_min,age_band_1024,age_band_description
from staging.provsrvlocspec a, provider_service_locations b, staging.provlddelta d
where a.pin = b.pin
and a.service_location_no = b.service_location_number
and a.pin = d.pin
 and d.service_location_number = b.service_location_number
and d.pin = b.pin
and d.change_type in( 'ADD','UPDATE');

commit;
