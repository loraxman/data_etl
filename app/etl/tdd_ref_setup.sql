-------------------------------------
-- TRUNCATE TEST TABLES 
-------------------------------------

TRUNCATE TABLE vault.h_customer cascade; --given
TRUNCATE TABLE vault.s_customer_raw cascade; --given
TRUNCATE TABLE vault.s_customer_master cascade; --then

-------------------------------------
-- GIVEN
-------------------------------------

-- insert test data
INSERT INTO vault.h_customer(customerkey,customerid)
SELECT 111,'AAA'
UNION
SELECT 222,'BBB';

-- insert test data
INSERT INTO vault.s_customer_raw(customerkey, recefftime, recloadtime, recloaduser, customerfirstname, customerlastname, customeraddress, customercity, customerstate, customerzip)
SELECT 111, now(), now(), current_user, 'Elvis','Presley', '345 happyhippo ave', 'Memphis', 'TN', '23113'
UNION
SELECT 222, now(), now(), current_user, 'Lisa-Marie','Presley', ' 1234 greenville street      ', 'McKinney', 'TX', '25678';

select 'PASS';