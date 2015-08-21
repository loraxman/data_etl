
--delete all that have changed since it is a full replace
delete from provider_languages using staging.provlddelta d, providers a
where  d.pin = a.pin
and a.id = provider_languages.provider_id;


insert into provider_languages
(provider_id, language_code, language_description)
select distinct b.id, language_code,language_description
 from staging.provlang a, providers b, staging.provlddelta d
 where a.pin = b.pin
and a.pin = d.pin
and d.change_type in( 'ADD','UPDATE');


 commit;
