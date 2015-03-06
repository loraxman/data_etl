DROP TABLE if exists staging.claims;

create table staging.claims(
sp_pin varchar(1024),       
srv_date_start  varchar(1024),        
srv_date_end    varchar(1024),        
hierarchy   varchar(1024),      
source_system   varchar(1024),      
plan_code   varchar(1024),      
member_id   varchar(1024),      
trans_id    varchar(1024),   
seg_num varchar(1024),      
pcp_tin varchar(1024),      
sp_tin  varchar(1024),      
sp_street1  varchar (1024),
sp_street2  varchar(1024),
sp_city varchar (1024),    
sp_state    varchar(1024),      
sp_zip  varchar(1024),      
sp_type varchar(1024),      
sp_scode    varchar(1024),      
pp_cod  varchar(1024),      
line_proc_code  varchar(1024),      
line_proc_mod_1 varchar(1024),      
srv_place   varchar(1024),      
ub92_rev_ctr    varchar(1024),      
ub92_bill_type  varchar(1024),      
service_units   varchar(1024),     
net_expense varchar(1024),     
amt_allowed varchar(1024),     
amt_paid    varchar(1024),     
ntl_drug_code   varchar(1024),      
cumb_id varchar(1024),      
sp_npi  varchar(1024),      
line_proc_mod_2 varchar(1024),      
line_proc_mod_3 varchar(1024),      
diag_code_01    varchar(1024),      
diag_code_02    varchar(1024),      
diag_code_03    varchar(1024),      
diag_code_04    varchar(1024),      
diag_code_05    varchar(1024),      
diag_code_06    varchar(1024),      
diag_code_07    varchar(1024),      
diag_code_08    varchar(1024),      
diag_code_09    varchar(1024),      
diag_code_10    varchar(1024),      
end_of_rec_01   varchar(1024)); 


COPY staging.claims FROM '/home/ubuntu/data_projects/data_etl/claimraw.dat' USING DELIMITERS '|' ;
commit;

select 'PASS';
