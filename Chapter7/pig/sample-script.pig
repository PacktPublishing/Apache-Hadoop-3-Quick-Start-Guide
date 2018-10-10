records = LOAD 'student-grades.csv' USING PigStorage(',') AS (year:int,name:chararray,grade:chararray);
filtered_records = FILTER records BY year == 2018 AND(grade matches 'A*');
DUMP filtered_records;
