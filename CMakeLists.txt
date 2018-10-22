cmake_minimum_required(VERSION 3.10)
project(Advanced_DBMS_Implementation C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(Advanced_DBMS_Implementation
        main.c
        supportFunctions.c
        supportFunctions.h
        radixHashJoin.c
        radixHashJoin.h)
target_link_libraries(Advanced_DBMS_Implementation m)