
truncate table w_h_provdrlocnpractspecl;

insert into w_h_provdrlocnpractspecl(provdrlocnpractspeclprovdrid,
provdrlocnpractspecllocnid,
provdrlocnpractspeclpractspeclcode )
select pin, service_location_no,
practice_code
 from provsrvlocspec where service_location_no != '0000000'  ;
select load_h_provdrlocnpractspecl();
commit;


select 'PASS' as status;
