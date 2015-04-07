truncate table etl.w_l_provdr_lang;



insert into etl.w_l_provdr_lang
(
provdrkey,
langkey,
receventtime
  )
select distinct provdrkey,langkey,current_timestamp
from vault.h_lang a, vault.h_provdr b,
staging.provlang c
where a.langcode = c.language_code
and trim(b.provdrid) = trim(c.pin);

select etl.load_l_provdr_lang();
commit;

select 'PASS';
