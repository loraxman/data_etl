

--truncate  staging.keen_json_procedure_searched ;
--truncate  staging.keen_json_signed_up ;
--truncate  staging.keen_json_signed_in ;
--truncate  staging.keen_json_user ;
--truncate  staging.keen_json_user_completes_provider_review ;
truncate  staging.wm_digest_to_member_id_mapping;
commit;
select 'PASS';
