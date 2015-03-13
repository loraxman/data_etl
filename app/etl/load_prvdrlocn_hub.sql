
truncate table etl.w_h_provdrlocn;

insert into etl.w_h_provdrlocn(provdrlocnprovdrid,provdrlocnid )
select pin, service_location_number from staging.provsrvloc where service_location_number != '0000000'  ;
select etl.load_h_provdrlocn();
commit;


select 'PASS' as status;
