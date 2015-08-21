
--delete all that have changed since it is a full replace
delete from provider_service_location_par_networks  using staging.provlddelta d, provider_service_locations b
where  d.pin = b.pin
 and d.service_location_number = b.service_location_number
and b.id = provider_service_location_par_networks.provider_service_location_id;


insert into provider_service_location_par_networks
(id,provider_service_location_id,pin,network_id,
service_location_no,practice_code,category_code,category_hierarchy,
master_category_code,service_grouping_type,service_grouping_code,
current_tier,current_tier_expiration_date,
future_tier,future_tier_effective_date,
designation_code,reason_code)

select a.id,a.provider_service_location_id,
a.pin,base_net_id_no,service_location_no,
practice_code,category_code,
null,master_category_code
,null,null,current_tier,
null,null,
null,null,null
from staging.srvgrppabbrev a,
staging.provlddelta d
where a.pin = d.pin
 and cast( d.service_location_number as integer) = cast(a.service_location_no as integer)
and d.change_type in( 'ADD','UPDATE');



commit;
