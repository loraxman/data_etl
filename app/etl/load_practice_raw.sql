
DROP TABLE if exists staging.practice;



CREATE TABLE staging.practice
(
  "Codes" text,
  "AMA Specialty Group" text,
  "Practice Code" text,
  "Practice Description" text
);


COPY staging.practice FROM '/Users/A727200/proj/ipynotes/work/provider/cleanpract.csv' USING DELIMITERS '|' ;
commit;

select 'PASS';
