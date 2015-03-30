truncate table etl.w_s_qualityprogram;



insert into etl.w_s_qualityprogram
(qualityprogramkey,
qualityprogramdescr,
receventtime
  )
 select distinct qualityprogramkey, flag_description,current_timestamp
 from staging.provsrvlocflg a,
 vault.h_qualityprogram b
 where a.flag_code = b.qualityprogramcode;
 
select etl.load_h_qualityprogram();
commit;


select 'PASS' as status;