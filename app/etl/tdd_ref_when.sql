-------------------------------------
-- WHEN
-------------------------------------


    truncate table etl.w_s_customer_master cascade;

    insert into etl.w_s_customer_master
    select customerkey, 
           now(),
           customeraddress || ',' || customercity || ',' || customerstate || ',' || customerzip,
           customerfirstname || customerlastname 
      from vault.s_customer_raw;

    select etl.load_s_customer_master();

/*** completed passing logic
    insert into etl.w_s_customer_master
    select customerkey, 
           now(),
           initcap(ltrim(rtrim(customeraddress))) || ', ' || initcap(ltrim(rtrim(customercity))) || ', ' || upper(ltrim(rtrim(customerstate))) || '  ' || ltrim(rtrim(customerzip)),
           initcap(ltrim(rtrim(customerfirstname))) || ' ' || initcap(ltrim(rtrim(customerlastname))) 
      from vault.s_customer_raw;
***/

    select etl.load_s_customer_master();


CREATE TEMP TABLE actual_results AS
SELECT customerkey,customerfulladdress 
  FROM vault.s_customer_master;

select 'PASS';