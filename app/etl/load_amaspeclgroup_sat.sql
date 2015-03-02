
truncate table etl.w_s_AmaSpeclGroup;

insert into etl.w_s_AmaSpeclGroup
(amaspeclgroupkey,
amaspeclgroupdescr,
recefftime
  )
 select distinct amaspeclgroupkey, amaspeclgroupcode,current_timestamp
 from vault.h_practspecl a,
 vault.h_amaspeclgroup b,
 staging.practice c
 where b.amaspeclgroupcode = "AMA Specialty Group";
 
select etl.load_s_AmaSpeclGroup();
commit;

select 'PASS' as status;