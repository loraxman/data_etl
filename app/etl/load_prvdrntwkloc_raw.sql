

DROP TABLE if exists staging.provntwkloc;

CREATE TABLE staging.provntwkloc
(
  pin character varying(1024),
  base_net_id_no character varying(1024),
  base_net_name character varying(1024),
  base_net_product character varying(1024),
  clone_net_id_no character varying(1024),
  addr_no character varying(1024),
  provprt_id character varying(1024),
  hospprt_id character varying(1024),
  accepting_new_patients character varying(1024),
  network_location_status character varying(1024),
  addr_print_ind character varying(1024),
  hct_estimate_allowed character varying(1024),
  hct_estimate_error_code character varying(1024),
  fpt_estimate_allowed character varying(1024),
  fpt_estimate_error_code character varying(1024),
  mpe_estimate_allowed character varying(1024),
  mpe_estimate_error_code character varying(1024),
  ppt_estimate_allowed character varying(1024),
  network_hierarchy character varying(1024),
  tier_code character varying(1024),
  fillertrail character varying(1024)
);


COPY staging.provntwkloc FROM '/Users/A727200/proj/data_etl/provntwkloc.dat' USING DELIMITERS '|' ;
commit;

select 'PASS';