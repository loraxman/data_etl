
truncate table w_h_provdrlocn;

insert into w_h_provdrlocn(provdrid,provdrlocnid )
select pin, service_location_number from provsrvloc where service_location_number != '0000000'  ;
select load_h_provdrlocn();
commit;


select 'PASS' as status;
