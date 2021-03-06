--have to drop some staging tables due to need to load without index
--index gets created later
-- index on Pin,srvloceno is a must due to search table build
drop  table staging.srvgrppabbrev cascade;

CREATE TABLE staging.srvgrppabbrev
(
  pin character varying(1024),
  base_net_id_no character varying(1024),
  service_location_no character varying(1024),
  practice_code character varying(1024),
  category_code character varying(1024),
  master_category_code character varying(1024),
  current_tier character varying(1024),
  provider_service_location_id integer,
  id integer
)
WITH (
  OIDS=FALSE
);

---truncate table staging.srvgrppabbrev;
---truncate table staging.provsrvloc;


DROP TABLE staging.provsrvloc;

CREATE TABLE staging.provsrvloc
(
  pin character varying(1024),
  info_type character varying(1024),
  provider_type character varying(1024),
  major_classification character varying(1024),
  behavioral_health_ind character varying(1024),
  opp_ind character varying(1024),
  accept_new_patients_ind character varying(1024),
  med_dent_ind character varying(1024),
  accepts_medicare character varying(1024),
  name character varying(1024),
  salutation character varying(1024),
  last_name character varying(1024),
  first_name character varying(1024),
  middle_name character varying(1024),
  rank character varying(1024),
  gender_description character varying(1024),
  birth_date character varying(1024),
  designation_code character varying(1024),
  service_location_number character varying(1024),
  primary_serv_loc_ind character varying(1024),
  addr_par_status character varying(1024),
  addr_print_ind character varying(1024),
  addr_nap_only_ind character varying(1024),
  addr_cust_only_ind character varying(1024),
  primary_phone_no character varying(1024),
  secondary_phone_no character varying(1024),
  fax_no character varying(1024),
  npi character varying(1024),
  hearing_product_code character varying(1024),
  nabp_number character varying(1024),
  primary_degree_desc character varying(1024),
  secondary_degree_desc character varying(1024),
  docfind_class_description character varying(1024),
  svcl_building character varying(1024),
  svcl_street1 character varying(1024),
  svcl_street2 character varying(1024),
  svcl_street3 character varying(1024),
  svcl_street4 character varying(1024),
  svcl_street5 character varying(1024),
  svcl_city character varying(1024),
  svcl_county character varying(1024),
  svcl_state character varying(1024),
  svcl_zip character varying(1024),
  svcl_xzip character varying(1024),
  svcl_country_cd character varying(1024),
  svcl_country_nm character varying(1024),
  svcl_handicap character varying(1024),
  svcl_latitude character varying(1024),
  svcl_longitude character varying(1024),
  fillertrail character varying(255),
  id integer
)
WITH (
  OIDS=FALSE
);

truncate table staging.provsrvlocspec;
truncate table staging.provlang;
truncate table staging.provsrvlocflg;
truncate table staging.hospaff;
truncate table staging.provlddelta;
truncate table staging.provntwkspec;
truncate table staging.provntwkloc;


