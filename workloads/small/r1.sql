drop table if exists r1;
CREATE TABLE r1 (c0 bigint,c1 bigint,c2 bigint);
LOAD DATA local INFILE '/home/lampros/Project/Advanced-DBMS-Implementation/workloads/small/r1.tbl' INTO TABLE r1 FIELDS TERMINATED BY '|';
