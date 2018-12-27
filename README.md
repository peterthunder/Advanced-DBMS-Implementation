# Advanced-DBMS-Implementation
Project for course Κ23a "Software Development for Information Systems".

This project uses the [RadixHashJoin methodology](https://ieeexplore.ieee.org/document/6544839) to Join two Tables.

## How to run
Navigate to the root-directory of this project and run the commands of your preferred choice:
1) For **BASIC** (fast) execution run:
    1) `make BASIC`
    2) `./radixHashJoin_basic`
2) For **DEBUG** (with prints) execution run:
    1) `make DEBUG`
    2) `./radixHashJoin_debug`

## Unit-testing
We apply unit-testing in our code by using the ["Unity" Framework](https://github.com/ThrowTheSwitch/Unity), which we include directly in "***UnitTesting/Unity-master***".
For **"Unity"** to work, we first need to install "**ruby**", "**rake**", "**libc6-dev-i386**", "**g++**" and "**g++-multilib**" by running the provided installer-script:<br/>
`./scripts/installUnitTestingDependencies.sh`

Then, in order to run the tests, you have to run the following command:<br/>
`make UNIT_TESTING`


# How was implemented
The program was implemented in three stages.
1) In the first stage, we worked in a sub-set of the project which was receiving some static data from two in-memory-tables and then used the [RadixHashJoin methodology](https://ieeexplore.ieee.org/document/6544839) to Join those two Tables. In the end the result of this join was printed on the monitor.<br/>
2) In the second stage, we expanded the program to receive dynamic data from multiple in-storage-tables and load them in memory. Then 

## Load tables and execute queries from "workloads/small"
