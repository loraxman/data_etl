

begin;


CREATE OR REPLACE VIEW provider_location_search AS 
 SELECT provider_data1.provider_loc_search.provider_service_location_id,
    provider_data1.provider_loc_search.provdrjson ->> 'provider_type'::text AS provider_type,
    provider_data1.provider_loc_search.geom AS geocode_gis,
    provider_data1.provider_loc_search.provdrjson ->> 'pin'::text AS pin,
    provider_data1.provider_loc_search.bundles,
    provider_data1.provider_loc_search.specialties,
    provider_data1.provider_loc_search.tier1_cat,
    provider_data1.provider_loc_search.tier2_cat,
    provider_data1.provider_loc_search.tier3_net,
    provider_data1.provider_loc_search.allnet,
    btrim(provider_data1.provider_loc_search.provdrjson ->> 'gender'::text) AS gender,
    provider_data1.provider_loc_search.languages
   FROM provider_data1.provider_loc_search;

GRANT SELECT ON TABLE provider_location_search TO public;


CREATE OR REPLACE VIEW provider_participation_networks AS 
 SELECT provider_data1.provider_service_location_par_networks.id,
    provider_data1.provider_service_location_par_networks.provider_service_location_id,
    provider_data1.provider_service_location_par_networks.pin,
    provider_data1.provider_service_location_par_networks.network_id,
    provider_data1.provider_service_location_par_networks.service_location_no,
    provider_data1.provider_service_location_par_networks.category_code,
    provider_data1.provider_service_location_par_networks.current_tier AS tier
   FROM provider_data1.provider_service_location_par_networks;

GRANT SELECT ON TABLE provider_participation_networks TO public;


CREATE OR REPLACE VIEW provider_service_location_categories AS 
 SELECT DISTINCT claim_book_of_records.bundle_id,
    a.network_id,
    a.category_code,
    a.provider_service_location_id,
    a.practice_code
   FROM provider_data1.provider_service_location_par_networks a
     JOIN claim_book_of_records ON a.practice_code::text = claim_book_of_records.practice_code::text AND a.category_code::text = 'AEX'::text AND (a.current_tier::text = ANY (ARRAY['2'::character varying, '1'::character varying]::text[]))
UNION
 SELECT DISTINCT 0 AS bundle_id,
    a.network_id,
    a.category_code,
    a.provider_service_location_id,
    a.practice_code
   FROM provider_data1.provider_service_location_par_networks a
  WHERE a.category_code::text <> 'AEX'::text AND (a.current_tier::text = ANY (ARRAY['2'::character varying, '1'::character varying]::text[]));

GRANT SELECT ON TABLE provider_service_location_categories TO public;


CREATE OR REPLACE VIEW provider_service_location_quality_programs AS 
 SELECT b.id,
    b.provider_service_location_id,
    a.quality_program_code,
    a.quality_program_description
   FROM provider_quality_programs a,
    provider_data1.provider_service_location_programs b
  WHERE a.quality_program_code = b.flag_code::text;


CREATE OR REPLACE VIEW provider_service_location_specialty_codes AS 
 WITH cbor_providertype_practicecode AS (
         SELECT DISTINCT claim_book_of_records.provider_type,
            claim_book_of_records.practice_code
           FROM claim_book_of_records
        )
 SELECT provider_data1.provider_service_location_specialties.provider_service_location_id,
    provider_data1.provider_service_location_specialties.practice_code,
    provider_data1.provider_service_location_specialties.practice_description,
    provider_data1.provider_service_location_specialties.prim_spec_ind AS is_primary_specialty,
    provider_data1.provider_service_location_specialties.service_location_no AS service_location_number,
        CASE
            WHEN cbor_providertype_practicecode.provider_type IS NULL THEN provider_data1.provider_service_location_specialties.practice_code
            ELSE cbor_providertype_practicecode.provider_type
        END AS provider_type
   FROM provider_data1.provider_service_location_specialties
     LEFT JOIN cbor_providertype_practicecode ON provider_data1.provider_service_location_specialties.practice_code::text = cbor_providertype_practicecode.practice_code::text;

GRANT SELECT ON TABLE provider_service_location_specialty_codes TO public;


alter table provider_hospital_affilliations set schema provider_data2;
alter table provider_languages  set schema provider_data2;
alter table  provider_loc_search set  schema provider_data2;
alter table provider_service_location_par_networks  set schema provider_data2;
alter table provider_service_location_programs  set schema provider_data2;
alter table provider_service_location_specialties  set schema provider_data2;
alter table provider_service_locations  set schema provider_data2;
alter table providers   set schema provider_data2;

CREATE OR REPLACE VIEW provider_hospital_affilliations
as
select * from provider_data1.provider_hospital_affilliations;

GRANT SELECT ON TABLE provider_hospital_affilliations to public;


CREATE OR REPLACE VIEW provider_languages
as
select * from provider_data1.provider_languages;

GRANT SELECT ON TABLE provider_languages to public;


CREATE OR REPLACE VIEW provider_loc_search
as
select * from provider_data1.provider_loc_search;

GRANT SELECT ON TABLE provider_loc_search to public;

CREATE OR REPLACE VIEW provider_service_location_par_networks
as
select * from provider_data1.provider_service_location_par_networks;

GRANT SELECT ON TABLE provider_service_location_par_networks to public;

CREATE OR REPLACE VIEW provider_service_location_programs
as
select * from provider_data1.provider_service_location_programs;

GRANT SELECT ON TABLE provider_service_location_programs to public;

CREATE OR REPLACE VIEW provider_service_location_specialties
as
select * from provider_data1.provider_service_location_specialties;

GRANT SELECT ON TABLE provider_service_location_specialties to public;

CREATE OR REPLACE VIEW provider_service_locations
as
select * from provider_data1.provider_service_locations;

GRANT SELECT ON TABLE provider_service_locations to public;

CREATE OR REPLACE VIEW providers
as
select * from provider_data1.providers;

GRANT SELECT ON TABLE providers to public;

CREATE OR REPLACE VIEW provider_service_location_network_specialties
as
select * from provider_data1.provider_service_location_network_specialties;

GRANT SELECT ON TABLE provider_service_location_network_specialties to public;
CREATE OR REPLACE VIEW provider_service_location_network_products
as
select * from provider_data1.provider_service_location_network_products;

GRANT SELECT ON TABLE provider_service_location_network_products to public;
commit;
