cmake_minimum_required (VERSION 3.10.2)
project (Advanced_DBMS_Implementation)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# set(CMAKE_C_COMPILER /usr/local/bin/gcc)
# set(CMAKE_CXX_COMPILER /usr/local/bin/g++)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address -Wall -pthread")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
find_package (Threads)

include_directories(include)

add_library(database
        UnitTesting/Unity-master/src/unity.c
        UnitTesting/Unity-master/src/unity.h
        UnitTesting/Unity-master/src/unity_internals.h
        UnitTesting/Unity-master/Testing/unity_config.h
        UnitTesting/Unity-master/Testing/MyTests/test/test_support_functions.c
        UnitTesting/Unity-master/Testing/MyTests/test/test_support_functions.h
        UnitTesting/Unity-master/Testing/MyTests/test/test_file_io.c
        UnitTesting/Unity-master/Testing/MyTests/test/test_file_io.h
        UnitTesting/Unity-master/Testing/MyTests/test/test_radix_hash_join.c
        UnitTesting/Unity-master/Testing/MyTests/test/test_radix_hash_join.h
        UnitTesting/Unity-master/Testing/MyTests/test/test_query_parsing.c
        UnitTesting/Unity-master/Testing/MyTests/test/test_query_parsing.h
        UnitTesting/Unity-master/Testing/MyTests/test/test_query_execution.c
        UnitTesting/Unity-master/Testing/MyTests/test/test_query_execution.h
        UnitTesting/Unity-master/Testing/MyTests/test/TestProductionCode.c
        src/file_io.c
        src/file_io.h
        src/parser.c
        src/parser.h
        src/query_functions.c
        src/query_functions.h
        src/radixHashJoin.c
        src/radixHashJoin.h
        src/supportFunctions.c
        src/supportFunctions.h
        src/struct_and_const_definitions.h
        src/statistics_functions.c
        src/statistics_functions.h
        src/threadpool.c
        src/threadpool.h)

target_include_directories(database PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        PRIVATE src)

add_executable(Driver src/main.c)
target_link_libraries(Driver database ${CMAKE_THREAD_LIBS_INIT})

# Test harness
add_executable(harness harness.cpp)

ADD_CUSTOM_TARGET(link_target ALL
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/workloads
        ${CMAKE_CURRENT_BINARY_DIR}/workloads)
