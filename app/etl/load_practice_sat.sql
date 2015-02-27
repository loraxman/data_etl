truncate table etl.w_s_practspecl;

insert into etl.w_s_practspecl
(practspeclkey,
  practspecldescr ,
  recefftime 
  )
select distinct b.practspeclkey,
"Practice Description",
current_timestamp
 from staging.practice a, vault.h_practspecl b
where "Practice Code" = b.practspeclcode;

select etl.load_s_practspecl();
commit;


truncate table etl.w_s_amaspeclgroup;

insert into etl.w_s_amaspeclgroup
(amaspeclgroupkey,
  amaspeclgroupdescr ,
  recefftime 
  )
select distinct b.amaspeclgroupkey,
"AMA Specialty Group",
current_timestamp
 from staging.practice a, vault.h_amaspeclgroup b
where "AMA Specialty Group" = b.amaspeclgroupcode;

select etl.load_s_amaspeclgroup();
commit;


select 'PASS' as status;