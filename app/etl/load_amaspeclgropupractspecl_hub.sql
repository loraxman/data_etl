
truncate table w_l_AmaSpeclGroupPractSpecl_amaspeclgroup;

insert into w_l_AmaSpeclGroupPractSpecl_amaspeclgroup
(AmaSpeclGroupPractSpeclKey,
amaspeclgroupkey,
Recefftime
  )
 select AmaSpeclGroupPractSpeclKey,amaspeclgroupkey,current_timestamp
 from h_AmaSpeclGroupPractSpecl a,
 h_amaspeclgroup b
 
where  b.amaspeclgroupcode = a.amaspeclgroupcode ;
 

 
select load_l_AmaSpeclGroupPractSpecl_amaspeclgroup();
commit;

