
truncate table etl.w_h_AmaSpeclGroup;

insert into etl.w_h_AmaSpeclGroup
(amaspeclgroupcode
  )
 select distinct amaspeclgroupcode
 from vault.h_practspecl a,
 vault.h_amaspeclgroup b,
 staging.practice c
 where b.amaspeclgroupcode = "AMA Specialty Group";
 
select etl.load_h_AmaSpeclGroup();
commit;

select 'PASS' as status;