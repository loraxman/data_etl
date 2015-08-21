DROP TABLE if exists staging.provlddelta;

CREATE TABLE staging.provlddelta
(
  pin character varying(1024),
  service_location_number varchar(1024),
  change_type varchar(1024),
  change_date varchar(1024),
  consumed_date varchar(1024),
  fillertrail varchar(1024));


COPY staging.provlddelta FROM PROGRAM  'zcat /home/ubuntu/data_projects/util/provlddelta.dat.gz' USING DELIMITERS '|' ;
commit;

select 'PASS';