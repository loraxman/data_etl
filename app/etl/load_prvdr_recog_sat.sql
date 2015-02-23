truncate table w_s_provdrqltyrecog;
insert into w_s_provdrqltyrecog (provdrqltyrecogkey,recefftime,
  provdrqltyrecogdescr)
select distinct b.provdrqltyrecogkey, current_timestamp, flag_description 
from provsrvlocflg a,
h_provdrqltyrecog b
where b.provdrqltyrecogcode = a.flag_code;

select load_s_provdrqltyrecog();
commit;

select 'PASS' as status;