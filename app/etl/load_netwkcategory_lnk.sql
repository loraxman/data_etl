

truncate table etl.w_l_netwkcategory_netwkmstrcategory;

insert into etl.w_l_netwkcategory_netwkmstrcategory
(netwkcategorycodekey,
netwkmstrcategory
  )
 select distinct category_code
 from staging.srvgrpprovass a
 netwkcategory b,
 netwkmstrcategory c
 where a.category_code = b.netwkcategorycode
 and a.master_category_code = a.netwkmstrcategorycode;
 
 
select etl.load_l_netwkcategory_netwkmstrcategory();
commit;


select 'PASS' as status;