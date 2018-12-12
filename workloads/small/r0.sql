drop table if exists r0;
CREATE TABLE r0 (c0 bigint,c1 bigint,c2 bigint);
LOAD DATA local INFILE '/home/lampros/Project/Advanced-DBMS-Implementation/workloads/small/r0.tbl' INTO TABLE r0 FIELDS TERMINATED BY '|';
