set client_encoding to 'latin1';

COPY staging.wm_digest_to_member_id_mapping FROM '/home/ubuntu/data_projects/data_etl/app/etl/digest_to_member_id_mapping.txt' delimiter ',';

commit;

