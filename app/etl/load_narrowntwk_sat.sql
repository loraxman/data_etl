truncate table etl.w_s_narrownetwk;

insert into etl.w_s_narrownetwk
(narrownetwkkey,
narrownetwkdescr,
receventtime
  )
 select distinct narrownetwkkey, a.base_net_name, current_timestamp
 from staging.provntwkloc a, vault.h_narrownetwk b
 where a.base_net_id_no = b.narrownetwkcode ;
 
select etl.load_s_narrownetwk();
commit;

select 'PASS' as status;