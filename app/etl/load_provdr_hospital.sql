truncate table etl.w_h_provdrhospital;



insert into etl.w_h_provdrhospital;
(provdrhospitalprovdrid,provdrhospitalhospitalid)
select distinct pin, hospital_pin
from staging.hospaff a, vault.h_provdr b, vault.h_provdr c
where a.pin = b.provdrid
and a.hospital_pin = c.provdrid;
 
select etl.load_h_provdrhospital();


truncate table etl.w_s_provdrhospital;



insert into etl.w_s_provdrhospital;
(provdrhospitalkey,
provdrhospitaladmitpriviledgeflag ,
provdrhospitalaffilstatuscode,
receventtime)
select provdrhospitalkey,admit_privileges, affil_status_code,current_timestamp
from staging.hospaff a, vault.h_provdrhospital b
where a.pin = b.provdrhospitalprovdrid
and a.hospital_pin = b.provdrhospitalhospitalid;
 
select etl.load_h_provdrhospital();


truncate table etl.w_l_provdrhospital_provdr_provdr;
insert into etl.w_l_provdrhospital_provdr_provdr
(provdrhospitalkey,
provdrkey_provdr,
receventtime)
select distinct  provdrhospitalkey,provdrkey, current_timestamp
from hospaff a, vault.h_provdrhospital b, vault.h_provdr c
where a.pin = b.provdrhospitalprovdrid
and a.hospital_pin = b.provdrhospitalhospitalid
and c.provdrid = b.provdrhospitalprovdrid;

select etl.load_l_provdrhospital_provdr_provdr();



truncate table etl.w_l_provdrhospital_provdr_hospital;
insert into etl.w_l_provdrhospital_provdr_hospital
(provdrhospitalkey,
provdrkey_hospital,
receventtime)
select distinct provdrhospitalkey,provdrkey, current_timestamp
from hospaff a, vault.h_provdrhospital b, vault.h_provdr c
where a.pin = b.provdrhospitalprovdrid
and a.hospital_pin = b.provdrhospitalhospitalid
and c.provdrid = b.provdrhospitalhospitalid;


select etl.load_l_provdrhospital_provdr_hospital();

