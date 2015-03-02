
truncate table etl.w_l_AmaSpeclGroupPractSpecl_amaspeclgroup;

insert into etl.w_l_AmaSpeclGroupPractSpecl_amaspeclgroup
(AmaSpeclGroupPractSpeclKey,
amaspeclgroupkey,
Recefftime
  )
 select AmaSpeclGroupPractSpeclKey,amaspeclgroupkey,current_timestamp
 from vault.h_AmaSpeclGroupPractSpecl a,
 vault.h_amaspeclgroup b
where  b.amaspeclgroupcode = a.amaspeclgroupcode ;
 

 
select etl.load_l_AmaSpeclGroupPractSpecl_amaspeclgroup();
commit;
select 'PASS';