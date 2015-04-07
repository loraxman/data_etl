truncate table w_s_provdr;

insert into w_s_provdr
(provdrkey,provdrtype,provdrisfacilityflag, provdrname, provdrsalutation,provdrlastname,provdrfirstname,
provdrmiddlename,provdrgender,provdrbirthdate,provdrprimarydegreedescr,provdrsecondarydegreedescr,recefftime
)
select distinct b.provdrkey, provider_type,
case when c."TYPE_CLASS_CD" = 'F' then 'Y'
else 'N'
end,
name,salutation,last_name,first_name,middle_name,gender_description,birth_date,primary_degree_desc,
secondary_degree_desc,current_timestamp
 from provsrvloc a, h_provdr b, provider_type c
where trim(a.pin)  = trim (b.provdrid )
and trim(a.provider_type) = trim(c."PROVIDER_TYPE_CD");

select load_s_provdr();
commit;

select 'PASS' as status;