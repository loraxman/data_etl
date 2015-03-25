

truncate table etl.w_h_netwkcategory;

insert into etl.w_h_netwkcategory
(netwkcategorycode
  )
 select distinct category_code
 from staging.srvgrpprovass ;
 
select etl.load_h_netwkcategory();
commit;


truncate table etl.w_h_netwkmstrcategory;

insert into etl.w_h_netwkmstrcategory
(netwkmstrcategorycode
  )
 select distinct master_category_code
 from staging.srvgrpprovass ;
 
select etl.load_h_netwkmstrcategory();
commit;


select 'PASS' as status;