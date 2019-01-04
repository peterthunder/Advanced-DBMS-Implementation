BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -O3 -ggdb -Wchkp -o radixHashJoin_basic src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/threadpool.c src/statistics_functions.c src/supportFunctions.c -pthread

BASIC_FOR_PROFILER:
	make CLEAN_BASIC_FOR_PROFILER
	@echo "Compile main...";
	gcc -O0 -ggdb -Wchkp -o radixHashJoin_basic_for_profiler src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/threadpool.c src/statistics_functions.c src/supportFunctions.c -pthread -pg

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -O3 -ggdb -Wall -Wchkp -o radixHashJoin_debug src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/threadpool.c src/statistics_functions.c src/supportFunctions.c -pthread -D PRINTING

DEEP_DEBUG:
	make CLEAN_DEEP_DEBUG
	@echo "Compile main...";
	gcc -O3 -ggdb -Wall -Wchkp -o radixHashJoin_deep_debug src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/threadpool.c src/statistics_functions.c src/supportFunctions.c -pthread -D DEEP_PRINTING

UNIT_TESTING:
	cd UnitTesting/Unity-master/Testing/MyTests && rake
	# Add more UnitTesting directories.

HARNESS:
	rm -rf build && ./compile.sh && ./runTestharness.sh

DRIVER:
	rm -rf build && ./compile.sh && ./run.sh

PROFILER:
	rm -rf profiler_output.txt
	make BASIC_FOR_PROFILER
	./radixHashJoin_basic_for_profiler && gprof radixHashJoin_basic_for_profiler gmon.out > profiler_output.txt && head -65 profiler_output.txt

EXAMPLE:
	gcc -o example example.c

CLEAN_BASIC:
	rm -rf radixHashJoin_basic

CLEAN_BASIC_FOR_PROFILER:
	rm -rf radixHashJoin_basic_for_profiler

CLEAN_DEBUG:
	rm -rf radixHashJoin_debug

CLEAN_DEEP_DEBUG:
	rm -rf radixHashJoin_deep_debug

CLEAN_ALL:
	make CLEAN_BASIC
	make CLEAN_BASIC_FOR_PROFILER
	make CLEAN_DEBUG
	make CLEAN_DEEP_DEBUG