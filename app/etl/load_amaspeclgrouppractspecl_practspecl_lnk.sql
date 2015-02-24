
truncate table w_l_amaspeclgrouppractspecl_practspecl;

insert into w_l_amaspeclgrouppractspecl_practspecl
(AmaSpeclGroupPractSpeclKey,
practspeclkey,
recefftime
  )
 select AmaSpeclGroupPractSpeclKey,practspeclkey, current_timestamp
 from h_AmaSpeclGroupPractSpecl a,
 h_practspecl b
 
where  b.practspeclcode = a.practspeclcode ;
 

 
select load_l_amaspeclgrouppractspecl_practspecl();
commit;

