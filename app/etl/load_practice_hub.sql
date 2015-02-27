
truncate table etl.w_h_practspecl;

insert into etl.w_h_practspecl(practspeclcode) select distinct "Practice Code" from 
staging.practice where "Practice Code" is not null
and "Practice Code" != 'Null';
select etl.load_h_practspecl();
commit;

truncate table etl.w_h_amaspeclgroup;

insert into etl.w_h_amaspeclgroup(amaspeclgroupcode) select distinct "AMA Specialty Group" 
from staging.practice where "AMA Specialty Group" is not null
and "AMA Specialty Group" != 'Null';
select etl.load_h_amaspeclgroup();
commit;


select 'PASS' as status;

