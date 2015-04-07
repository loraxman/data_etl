
truncate table etl.w_s_lang;



insert into etl.w_s_lang
(langkey,
langname,
receventtime
  )
 select  distinct langkey, language_description, current_timestamp
 from provlang a, vault.h_lang b
 where a.language_code = b.langcode;
 
 
select etl.load_s_lang();
commit;

select 'PASS';