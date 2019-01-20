# Advanced-DBMS-Implementation
Project for course Κ23a "Software Development for Information Systems".<br/>


# Authors
[Lampros Smyrnaios](https://github.com/LSmyrnaios)<br/>
[Petros Morfiris](https://github.com/peterthunder)<br/>
[Giorgos Theodosopoulos](https://github.com/gtheo91)<br/>


# Description
The task was to evaluate batches of join queries on a set of pre-defined relations.<br/>
Each join query specifies a set of relations, (equality) join predicates, and selections (aggregations).<br/>
The challenge was to execute the queries as fast as possible without (much) prior indexing.<br/>


# The Project
The project was split in three phases:<br/>
1) In the first phase, we worked in a sub-set of the project in which the program was receiving some static data from two in-memory-tables and then used the [RadixHashJoin methodology](https://ieeexplore.ieee.org/document/6544839) to Join those two Tables. In the end the result of this join was printed on the monitor.<br/>
2) In the second phase, we expanded the program to receive dynamic data from multiple in-storage-tables and load them in memory. We also added the ability to receive queries for the loaded data, execute them and return the results.<br/>
3) In the third phase, we implemented multi-threaded-execution. We also gathered statistics for each table we loaded as well as for each query we were about to execute. We used these statistics to optimize the query-execution by re-ordering the joins, thus reducing the amount of intermediate table results that are created while joining the tables.<br/>


## Extra optimizations we applied in the third phase
In the third part of the project we worked on extra optimizations to reduce the query-execution-time.<br/>
Those optimization where not inside the project guidelines.<br/>
We implemented the following extra optimizations:<br/>
1) Multi-threaded SUM-calculation.<br/>
2) Blocking the execution of individual queries which are guaranteed to produce zero-results. During statistics-gathering, when a filter of a query is guaranteed to produce zero-results, all other predicates will produce zero-results too, because the query is a connected-graph.<br/>
3) Remove duplicate joins inside the queries.<br/>


## Program's execution
When the program launches, it does the following:<br/>
1. Sets the I/O Streams.<br/>
2. Initializes the Thread-pool.<br/>
3. Loads the tables containing the data, into the main memory.<br/>
4. Creates the Relations-array (used for hosting the columns of the tables for joining them).<br/>
5. Creates the sums-structure (used for hosting the results from each query).<br/>
6. Receives and processes the queries.<br/>
7. De-allocates memory.<br/>


## Explanation of main program-phases.

### Table Loading
In order to load the tables, the program first receives the names of the tables and then for each table it loads its values in the main memory and calculate initial statistics.<br/>
The statistics, calculated for the numeric-data of each column of each table, are listed below:<br/>
1. The lower (min) value.<br/>
2. The upper (max) value.<br/>
3. The count of all the values.<br/>
4. The count of the distinct values.<br/>
We use these statistics later when we want to re-order the joins of a query to achieve faster query-execution.<br/>

### Process query
#### Parsing
For each query we receive, we parse it to extract its members (**FROM** - **WHERE** - **SELECT**) and save this information in a query-structure. The query members are those are:
1. The "**RelationIDs**" which are the names of the tables which participate in the query along with their aliases.<br/>
2. The "**Predicates**" which contain the filters and the joins based on which the results will be calculated.<br/>
3. The "**Selections**" containing the type of results that are asked. These results are the SUMs of of the values which (values) coming from the tables which participate in the query.<br/>
<br/>
#### Statistics & Best Tree Join-order
After the parsing, we gather statistics for the predicates in order to predict the best order in which the joins will be executed.<br/>
In order to achieve the best Join-order, we first gather the statistics for the filters and then we gather statistics for different Join-combinations and apply the one which is predicted to produce the least amount or intermediate results.<br/>
The new Join-order gets saved in the query-structure.<br/>
<br/>
#### Execution and results
Then, we execute the query by firstly executing the filters and creating intermediate tables to store the results produced by them.<br/>
Afterwards, we execute the joins in the order they exist in the query-structure (which order will be the best since it will have change as a result of the **Best-Tree** optimization).<br/>
In the end, we will have the produced results in the final intermediate tables. Those results will then get SUM-med depending on the given query-selections and those SUMs will be returned.<br/>
<br/>

### Experiments we did.... (H1, H2, MULTI-THREADING, JOINED_ROW-IDS_NUM)
When experimented with the H1 hash-parameter, we found that by assigning it to 3 we get the best execution-time.<br/>
