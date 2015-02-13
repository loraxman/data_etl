truncate table w_h_prvrdqltyrecog;
truncate table w_s_prvrdqltyrecog;

insert into w_h_prvrdqltyrecog(prvrdqltyrecogcode) select distinct flag_code from provsrvlocflg;
select load_h_prvrdqltyrecog();
commit;

insert into w_s_prvrdqltyrecog (prvrdqltyrecogkey,recefftime,
  prvrdqltyrecogdescr)
select distinct b.prvrdqltyrecogkey, current_timestamp, flag_description 
from provsrvlocflg a,
h_prvrdqltyrecog b
where b.prvrdqltyrecogcode = a.flag_code;

select load_s_prvrdqltyrecog();
commit;

select 'PASS' as status;
