cmake_minimum_required(VERSION 3.10)
project(Advanced_DBMS_Implementation C)

set(CMAKE_C_STANDARD 11)

add_executable(Advanced_DBMS_Implementation
        main.c)
target_link_libraries(Advanced_DBMS_Implementation m)