-------------------------------------
-- PUBLISH RESULTS 
-------------------------------------

-- Assert
CREATE TEMP TABLE test_results AS
SELECT * FROM actual_results
EXCEPT
SELECT * FROM expected_results;

SELECT COUNT(*) as res1 FROM test_results;

with eval1 as (
    SELECT COUNT(*) as res1 FROM test_results
),
eval2 as (
    SELECT COUNT(*) as res2 FROM actual_results
)
select 
 CASE WHEN eval1.res1 > 0 or eval2.res2 = 0  THEN 'FAIL'
            ELSE 'PASS'
       END
    from eval1, eval2
    ;
    

