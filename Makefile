BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -O3 -ggdb -Wchkp -o radixHashJoin_basic src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/statistics_functions.c src/supportFunctions.c -pg

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -O3 -ggdb -Wall -Wchkp -o radixHashJoin_debug src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/statistics_functions.c src/supportFunctions.c -D PRINTING

UNIT_TESTING:
	cd UnitTesting/Unity-master/Testing/MyTests && rake
	# Add more UnitTesting directories.

HARNESS:
	rm -rf build && ./compile.sh && ./runTestharness.sh

DRIVER:
	rm -rf build && ./compile.sh && ./run.sh

PROFILER:
	rm -rf profiler_output.txt && make BASIC && gprof radixHashJoin_basic > profiler_output.txt && head -28 profiler_output.txt

EXAMPLE:
	gcc -o example example.c

CLEAN_BASIC:
	rm -rf radixHashJoin_basic

CLEAN_DEBUG:
	rm -rf radixHashJoin_debug

CLEAN_ALL:
	rm -rf radixHashJoin_basic
	rm -rf radixHashJoin_debug
