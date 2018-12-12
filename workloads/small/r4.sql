drop table if exists r4;
CREATE TABLE r4 (c0 bigint,c1 bigint);
LOAD DATA local INFILE '/home/lampros/Project/Advanced-DBMS-Implementation/workloads/small/r4.tbl' INTO TABLE r4 FIELDS TERMINATED BY '|';
