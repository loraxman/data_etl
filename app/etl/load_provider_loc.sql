
insert into providers
(pin,provider_type,is_facility,provider_name,salutation,last_name,first_name,middle_name,gender
,birth_date,primary_degree_descr,secondary_degree_descr
)
select distinct
a.pin,provider_type,case when c."TYPE_CLASS_CD" = 'F' then 'Y'
else 'N'
end,name,salutation,last_name,first_name,middle_name,gender_description
,birth_date,primary_degree_desc,secondary_degree_desc
 from staging.provsrvloc a,provider_type c, staging.provlddelta d
where
 trim(a.provider_type) = trim(c."PROVIDER_TYPE_CD")
and a.pin = d.pin
and a.major_classification != 'GRP'
and d.change_type = 'ADD';


--now update for UPDATE records

update providers set pin=a.pin ,provider_type=a.provider_type,is_facility= case when c."TYPE_CLASS_CD" = 'F' then 'Y' else 'N' end ,
provider_name=name ,salutation=a.salutation ,last_name=a.last_name ,first_name=a.first_name,middle_name=a.middle_name ,gender=a.gender_description ,birth_date= a.birth_date,
primary_degree_descr=a.primary_degree_desc ,secondary_degree_descr=a.secondary_degree_desc

from staging.provsrvloc a, staging.provlddelta b, provider_type c
where providers.pin = a.pin
and b.pin = providers.pin
and b.change_type = 'UPDATE';



 insert into provider_service_locations
(id, provider_id,pin, is_facility,info_type,major_classification,behavioral_health_ind,opp_ind,accept_new_patients_ind,
med_dent_ind,accepts_medicare,rank,designation_code,service_location_number,primary_serv_loc_ind,addr_par_status,
addr_print_ind,addr_nap_only_ind,
addr_cust_only_ind,primary_phone_no,secondary_phone_no,fax_no,hearing_product_code,
nabp_number,docfind_class_description,building,street1,street2,street3,street4,street5
,city,county,state,zip,xzip,country_cd,country_nm,handicap,latitude,longitude,geocode_gis,npi
)
select a.id,
b.id,a.pin, b.is_facility,
info_type,major_classification,behavioral_health_ind,opp_ind,accept_new_patients_ind,
med_dent_ind,accepts_medicare,rank,designation_code,a.service_location_number,
primary_serv_loc_ind,addr_par_status,addr_print_ind,addr_nap_only_ind,addr_cust_only_ind,
primary_phone_no,secondary_phone_no,fax_no,hearing_product_code,nabp_number,docfind_class_description,
svcl_building,
svcl_street1,svcl_street2,svcl_street3,svcl_street4,svcl_street5,svcl_city,svcl_county,svcl_state,svcl_zip,svcl_xzip,svcl_country_cd
,svcl_country_nm,svcl_handicap,svcl_latitude,svcl_longitude,
ST_GeomFromText('POINT(' ||  cast(cast(svcl_longitude as float)  as varchar) || ' ' || svcl_latitude || ')',4326),
npi
 from staging.provsrvloc a,providers b, staging.provlddelta d
where
  a.pin = b.pin
 and d.pin = a.pin
and b.pin = d.pin
 and d.service_location_number = a.service_location_number
 and d.change_type = 'ADD';


update provider_service_locations set pin=a.pin ,is_facility=case when c."TYPE_CLASS_CD" = 'F' then 'Y' else 'N' end ,info_type=a.info_type ,
major_classification=a.major_classification,behavioral_health_ind=a.behavioral_health_ind ,opp_ind=a.opp_ind ,accept_new_patients_ind=a.accept_new_patients_ind
,med_dent_ind=a.med_dent_ind ,accepts_medicare=a.accepts_medicare ,rank=a.rank ,designation_code=a.designation_code
,service_location_number=a.service_location_number ,primary_serv_loc_ind=a.primary_serv_loc_ind
,addr_par_status=a.addr_par_status ,addr_print_ind=a.addr_print_ind
,addr_nap_only_ind=a.addr_nap_only_ind ,addr_cust_only_ind=a.addr_cust_only_ind
,primary_phone_no=a.primary_phone_no ,secondary_phone_no=a.secondary_phone_no ,fax_no=a.fax_no ,
hearing_product_code=a.hearing_product_code ,nabp_number=a.nabp_number ,docfind_class_description=a.docfind_class_description ,
building=svcl_building ,street1=svcl_street1 ,street2=svcl_street2 ,street3=svcl_street3 ,street4=svcl_street4 ,street5=svcl_street5 ,city=svcl_city
,county=svcl_county ,state=svcl_state ,
zip=svcl_zip ,xzip=svcl_xzip ,country_cd=svcl_country_cd ,country_nm=svcl_country_nm ,handicap=svcl_handicap ,latitude=svcl_latitude ,longitude=svcl_longitude ,geocode_gis=ST_GeomFromText('POINT(' ||  cast(cast(svcl_longitude as float)  as varchar) || ' ' || svcl_latitude || ')',4326)
from staging.provlddelta d, staging.provsrvloc a, provider_type c
where  d.pin = a.pin
 and d.service_location_number = a.service_location_number
and trim(a.provider_type) = trim(c."PROVIDER_TYPE_CD")
and d.change_type='UPDATE';


