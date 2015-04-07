

truncate table w_s_provdrlocn;

insert into w_s_provdrlocn(provdrlocnkey,provdrlocnprimarylocnflag,
provdrlocnaddressparstatus,
provdrlocnaddressprintflag,provdrlocnaddressnaponlyflag,
provdrlocnaddresscustonlyflag,provdrlocnprimaryphone,
provdrlocnsecondaryphone,provdrlocnfaxphone,provdrlocnnpi,provdrlocnhearingprodcode,
provdrlocnnabpnumber,provdrlocnbuilding,provdrlocnstreet1,provdrlocnstreet2,provdrlocnstreet3,
provdrlocnstreet4,provdrlocnstreet5,provdrlocncity,provdrlocnstate,provdrlocnzip,provdrlocnzipext,
provdrlocncounty,provdrlocncountrycode,
provdrlocnhandicapflag,provdrlocnlatitude,provdrlocnlongitude,
ProvdrBehavioralHealthFlag,
ProvdrLocnDisplayFlag,
recefftime)
select provdrlocnkey,
primary_serv_loc_ind,
addr_par_status,
addr_print_ind,
addr_nap_only_ind ,
addr_cust_only_ind ,
primary_phone_no,
secondary_phone_no,
fax_no,
npi,
hearing_product_code,
nabp_number,
svcl_building,
svcl_street1 ,
svcl_street2,
svcl_street3,
svcl_street4,
svcl_street5,
svcl_city,
svcl_state,
svcl_zip,
svcl_xzip,
svcl_county,
svcl_country_cd,
svcl_handicap ,
cast(svcl_latitude as double precision) ,
cast (svcl_longitude as double precision) ,
behavioral_health_ind,
addr_print_ind,
current_timestamp
from provsrvloc a, 
h_provdrlocn b
where b.provdrid = a.pin
and  b.provdrlocnid = a.service_location_number;
select load_s_provdrlocn();
commit;


select 'PASS' as status;