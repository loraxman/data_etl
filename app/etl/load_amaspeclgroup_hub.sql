

truncate table etl.w_h_AmaSpeclGroupPractSpecl;

insert into etl.w_h_AmaSpeclGroupPractSpecl
(amaspeclgroupcode,
practspeclcode
  )
 select distinct amaspeclgroupcode,practspeclcode
 from vault.h_practspecl a,
 vault.h_amaspeclgroup b,
 staging.practice c
 where "Practice Code" = a.practspeclcode
 and b.amaspeclgroupcode = "AMA Specialty Group";
 
select etl.load_h_AmaSpeclGroupPractSpecl();
commit;

select 'PASS' as status;