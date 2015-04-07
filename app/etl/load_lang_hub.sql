
truncate table etl.w_h_lang;



insert into etl.w_h_lang
(langcode
  )
 select distinct language_code
 from provlang;
 
select etl.load_h_lang();
commit;

select 'PASS';