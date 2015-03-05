
truncate table etl.w_h_provdr;

insert into etl.w_h_provdr(provdrid) select distinct pin from staging.provsrvloc;
select etl.load_h_provdr();
commit;


select 'PASS' as status;
