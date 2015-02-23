
truncate table w_h_provdr;

insert into w_h_provdr(provdrid) select distinct pin from provsrvloc;
select load_h_provdr();
commit;


select 'PASS' as status;
