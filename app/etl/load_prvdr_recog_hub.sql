
truncate table w_s_provdrqltyrecog;

insert into w_h_provdrqltyrecog(provdrqltyrecogcode) select distinct flag_code from provsrvlocflg;
select load_h_provdrqltyrecog();
commit;


select 'PASS' as status;
