
truncate table etl.w_h_qualityprogram;



insert into etl.w_h_qualityprogram
(qualityprogramcode
  )
 select distinct flag_code
 from staging.provsrvlocflg;
select etl.load_h_qualityprogram();
commit;


select 'PASS' as status;