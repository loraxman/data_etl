
--delete all that have changed since it is a full replace
delete from provider_service_location_network_specialties  using staging.provlddelta d
where  d.pin = provider_service_location_network_specialties.pin
 and cast(d.service_location_number as integer) = cast(provider_service_location_network_specialties.addr_no as integer);

insert into provider_service_location_network_specialties
(
  provider_service_location_id,pin,network_id,clone_net_id_no,addr_no,practice_type,practice_code,practice_description,rollup_code,rollup_description,role_pcp_spec_both_none,prim_spec_ind,board_certified_code,board_cert_expiration_year,credential_cd,age_band_min,age_band_1024,age_band_description,cap_office_number,practice_print_ind
)
select b.id,
a.pin,base_net_id_no,clone_net_id_no,addr_no,practice_type,practice_code,practice_description,rollup_code,rollup_description,role_pcp_spec_both_none,prim_spec_ind,board_certified_code,board_cert_expiration_year,credential_cd,age_band_min,age_band_1024,age_band_description,cap_office_number,practice_print_ind
from staging.provntwkspec a, provider_service_locations b, staging.provlddelta d
where a.pin = b.pin
and cast(a.addr_no as integer) = cast(b.service_location_number as integer)
and a.pin = d.pin
 and d.service_location_number = b.service_location_number
and d.change_type in( 'ADD','UPDATE');

delete from provider_service_location_network_products  using staging.provlddelta d
where  d.pin = provider_service_location_network_products.pin
 and cast(d.service_location_number as integer) = cast(provider_service_location_network_specialties.addr_no as integer);


insert into provider_service_location_network_products
(
provider_service_location_id,pin,base_net_id_no,base_net_name,base_net_product,clone_net_id_no,addr_no,provprt_id,hospprt_id,accepting_new_patients,network_location_status,addr_print_ind,hct_estimate_allowed,hct_estimate_error_code,fpt_estimate_allowed,fpt_estimate_error_code,mpe_estimate_allowed,mpe_estimate_error_code,ppt_estimate_allowed,network_hierarchy,tier_code)

select b.id, 
a.pin,base_net_id_no,base_net_name,base_net_product,clone_net_id_no,addr_no,provprt_id,hospprt_id,accepting_new_patients,network_location_status,a.addr_print_ind,hct_estimate_allowed,hct_estimate_error_code,fpt_estimate_allowed,fpt_estimate_error_code,mpe_estimate_allowed,mpe_estimate_error_code,ppt_estimate_allowed,network_hierarchy,tier_code
from staging.provntwkloc a, provider_service_locations b, staging.provlddelta d
  where a.pin = b.pin
and cast(a.addr_no as integer) = cast(b.service_location_number as integer)
and a.pin = d.pin
  and d.service_location_number = b.service_location_number
 and d.change_type in( 'ADD','UPDATE');
commit;
