
truncate table w_h_AmaSpeclGroupPractSpecl;

insert into w_h_AmaSpeclGroupPractSpecl
(amaspeclgroupcode,
practspeclcode
  )
 select amaspeclgroupcode,practspeclcode
 from h_practspecl a,
 h_amaspeclgroup b,
 practice c
 where "Practice Code" = a.practspeclcode
 and b.amaspeclgroupcode = "AMA Specialty Group";
 
select load_h_AmaSpeclGroupPractSpecl();
commit;

