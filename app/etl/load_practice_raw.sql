
DROP TABLE if exists practice;



CREATE TABLE practice
(
  "Codes" text,
  "AMA Specialty Group" text,
  "Practice Code" text,
  "Practice Description" text
);


COPY practice FROM '/Users/A727200/proj/ipynotes/work/provider/cleanpract.csv' USING DELIMITERS '|' ;
commit;

select 'PASS';
