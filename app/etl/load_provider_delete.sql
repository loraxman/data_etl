
--todo cleanup other child tables of service loc
delete from provider_service_locations using staging.provlddelta d
where  d.pin = provider_service_locations.pin
 and d.service_location_number = provider_service_locations.service_location_number
and d.change_type='DELETE';

delete from provider_languages using staging.provlddelta d, providers a
where  d.pin = a.pin
and a.id = provider_languages.id
and d.change_type='DELETE';

delete from providers where
pin in ( select pin from staging.provlddelta d where
d.change_type = 'DELETE');
commit;
