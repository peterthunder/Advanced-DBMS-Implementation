# Advanced-DBMS-Implementation
Project for course Κ23a "Software Development for Information Systems".


# Authors
[Lampros Smyrnaios](https://github.com/LSmyrnaios) - sdi1400310<br/>
[Petros Morfiris](https://github.com/peterthunder) - sdi1300106<br/>
[Giorgos Theodosopoulos](https://github.com/gtheo91) - sdi0900055<br/>


# How was implemented
The project was splitted in three parts:
1) In the first part, we worked in a sub-set of the project which was receiving some static data from two in-memory-tables and then used the [RadixHashJoin methodology](https://ieeexplore.ieee.org/document/6544839) to Join those two Tables. In the end the result of this join was printed on the monitor.<br/>
2) In the second part, we expanded the program to receive dynamic data from multiple in-storage-tables and load them in memory. Then out program receives the queries to execute.....
3) In the third part, we implemented multithreaded-execution. We also gathered statistics for each table we loaded as well as for each query we were about to execute. We used these statistics to optimize the query-execution by re-ordering the joins, thus reducing the amount of intermediate tables that are created.<br/>

#### Extra optimizations we applied in the third part
In the third part of the project we were asked to apply extra optimizations to reduce the execution-time.<br/>
So we implemented the following:
1) Multithreaded SUM-calculation.
2) Blocking the execution of individual queries which are guaranteed to produce zero-results. During statistics-gathering, when a filter of a query is guaranteed to produce zero-results, all other predicates will produce zero-results too, because the query is a connected-graph.
3) Remove duplicate joins inside the queries.




## Program's execution
When the  program launches, ....


#### Execute query..




#### Experiments we did.... (H1, H2, MULTITHREADING, JOINED_ROWIDS_NUM)






