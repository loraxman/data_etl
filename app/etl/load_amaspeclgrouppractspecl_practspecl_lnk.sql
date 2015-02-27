
truncate table etl.w_l_amaspeclgrouppractspecl_practspecl;

insert into etl.w_l_amaspeclgrouppractspecl_practspecl
(AmaSpeclGroupPractSpeclKey,
practspeclkey,
recefftime
  )
 select AmaSpeclGroupPractSpeclKey,practspeclkey, current_timestamp
 from vault.h_AmaSpeclGroupPractSpecl a,
 vault.h_practspecl b
 
where  b.practspeclcode = a.practspeclcode ;
 

 
select etl.load_l_amaspeclgrouppractspecl_practspecl();
commit;

