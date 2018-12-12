drop table if exists r3;
CREATE TABLE r3 (c0 bigint,c1 bigint,c2 bigint,c3 bigint);
LOAD DATA local INFILE '/home/lampros/Project/Advanced-DBMS-Implementation/workloads/small/r3.tbl' INTO TABLE r3 FIELDS TERMINATED BY '|';
