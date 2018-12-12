drop table if exists r2;
CREATE TABLE r2 (c0 bigint,c1 bigint,c2 bigint,c3 bigint);
LOAD DATA local INFILE '/home/lampros/Project/Advanced-DBMS-Implementation/workloads/small/r2.tbl' INTO TABLE r2 FIELDS TERMINATED BY '|';

