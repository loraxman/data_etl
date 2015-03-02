-------------------------------------
-- THEN
-------------------------------------

CREATE TEMP TABLE expected_results AS 
SELECT customerkey, customerfulladdress 
  FROM vault.s_customer_master WHERE 1 = 2; --just to get the structure/datatypes

INSERT INTO expected_results
SELECT 111,'345 Happyhippo Ave, Memphis, TN  23113'
UNION
SELECT 222,'1234 Greenville Street, Mckinney, TX  25678';

select 'PASS';