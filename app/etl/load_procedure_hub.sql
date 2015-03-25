
truncate table etl.w_h_procedure

insert into etl.w_h_procedure (procedurecode)
select distinct procedure_main from staging.proceduremapping;
select etl.load_h_procedure();
commit;


truncate table etl.w_s_procedure

insert into etl.w_s_procedure (procedurekey,receventtime)
select distinct procedurekey,procedure_main , current_timestamp from 
staging.proceduremapping a, 
vault.h_procedure b 
where a.procedure_main = b.procedurecode;

select etl.load_s_procedure();
commit;


(practspeclprocedurekey serial,
practspeclprocedurepractspeclcode varchar(1024) not null,
practspeclprocedureprocedurecode varchar(1024) not null)

truncate table etl.w_h_practspeclprocedure;

insert into etl.w_h_practspeclprocedure (practspeclprocedurepractspeclcode ,practspeclprocedureprocedurecode)
select distinct a.practice_code,procedure_main from 
staging.proceduremapping a;

select etl.load_h_practspeclprocedure();
commit;

truncate table etl.w_s_practspeclprocedure;


insert into vault.w_s_practspeclprocedure (practspeclprocedurekey,
receventtime, 
practspeclproceduresubcategorydescr,
practspeclprocedurecombinedterm 
)

select distinct practspeclprocedurekey a,current_timestamp,
procedure_sub, combined_term from 
staging.proceduremapping a,
vault.h_practspeclprocedure b
where a.practice_code = b.practspeclprocedurepractspeclcode
and a.procedure_main = b.practspeclprocedureprocedurecode;


select etl.load_s_practspeclprocedure();


truncate table etl.w_l_practspeclprocedure_procedure;
insert into  etl.w_l_practspeclprocedure_procedure

(practspeclprocedurekey ,
procedurekey ,
Receventtime)
select distinct practspeclprocedurekey,
procedurekey,
current_timestamp
from vault.h_practspeclprocedure a,
vault.h_procedure b
where a.practspeclprocedureprocedurecode  = b.procedurecode;




select etl.load_l_practspeclprocedure_procedure();
commit;


truncate table etl.w_l_practspeclprocedure_practspecl;
insert into  etl.w_l_practspeclprocedure_practspecl

(practspeclprocedurekey ,
practspeclkey ,
Receventtime)


select distinct practspeclprocedurekey,
practspeclkey,
current_timestamp
from vault.h_practspeclprocedure a,
vault.h_practspecl b
where a.practspeclprocedurepractspeclcode= b.practspeclcode;




select etl.load_l_practspeclprocedure_procedure();
commit;



select 'PASS' as status;
