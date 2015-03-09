

truncate table etl.w_h_narrownetwk;

insert into etl.w_h_narrownetwk
(narrownetwkcode
  )
 select distinct base_net_id_no
 from staging.provntwkloc ;
 
select etl.load_h_narrownetwk();
commit;

select 'PASS' as status;