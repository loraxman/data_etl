truncate table w_s_practspecl;

insert into w_s_practspecl
(practspeclkey,
  practspecldescr ,
  recefftime 
  )
select distinct b.practspeclkey,
"Practice Description",
current_timestamp
 from practice a, h_practspecl b
where "Practice Code" = b.practspeclcode;

select load_s_practspecl();
commit;


truncate table w_s_amaspeclgroup;

insert into w_s_amaspeclgroup
(amaspeclgroupkey,
  amaspeclgroupdescr ,
  recefftime 
  )
select distinct b.amaspeclgroupkey,
"AMA Specialty Group",
current_timestamp
 from practice a, h_amaspeclgroup b
where "AMA Specialty Group" = b.amaspeclgroupcode;

select load_s_amaspeclgroup();
commit;


select 'PASS' as status;