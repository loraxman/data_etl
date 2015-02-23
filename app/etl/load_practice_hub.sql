
truncate table w_h_practspecl;

insert into w_h_practspecl(practspeclcode) select distinct "Practice Code" from practice where "Practice Code" is not null
and "Practice Code" != 'Null';
select load_h_practspecl();
commit;

truncate table w_h_amaspeclgroup;

insert into w_h_amaspeclgroup(amaspeclgroupcode) select distinct "AMA Specialty Group" from practice where "AMA Specialty Group" is not null
and "AMA Specialty Group" != 'Null';
select load_h_amaspeclgroup();
commit;


select 'PASS' as status;

