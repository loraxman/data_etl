truncate table w_s_provdr;

insert into w_s_provdr
(provdrkey, provdrname, provdrsalutation,provdrfirstname,provdrlastname,
provdrmiddlename,provdrgender,provdrbirthdate,provdrprimarydegreedescr,provdrsecondarydegreedescr,recefftime
)
select distinct b.provdrkey,
name,salutation,last_name,first_name,middle_name,gender_description,birth_date,primary_degree_desc,
secondary_degree_desc,current_timestamp
 from provsrvloc a, h_provdr b
where a.pin = b.provdrid;

select load_s_provdr();
commit;

select 'PASS' as status;