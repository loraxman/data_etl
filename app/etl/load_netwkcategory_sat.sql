

truncate table etl.w_s_netwkcategory;

insert into etl.w_s_netwkcategory
(netwkcategorycode
  )
 select distinct category_description
 from staging.srvgrpprovass a,
  vault.h_netwkcategory b
  where a.category_code = b.netwkcategorycode ;
 
select etl.load_s_netwkcategory();
commit;


truncate table etl.w_s_netwkmstrcategory;

insert into etl.w_s_netwkmstrcategory
(netwkcategorycode
  )
 select distinct category_description
 from staging.srvgrpprovass a,
  vault.h_netwkmstrcategory b
  where a.category_code = b.netwkcategorycode ;
 
select etl.load_s_netwkmstrcategory();
commit;

select 'PASS' as status;



