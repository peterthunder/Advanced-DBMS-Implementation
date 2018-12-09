BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -g -o radixHashJoin_basic src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/supportFunctions.c -lm

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -g -o radixHashJoin_debug src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/supportFunctions.c -lm -D PRINTING

UNIT_TESTING:
	cd UnitTesting/Unity-master/Testing/RadixHashJoin_Test && rake
	# Add more UnitTesting directories.

CLEAN_BASIC:
	rm -rf radixHashJoin_basic

CLEAN_DEBUG:
	rm -rf radixHashJoin_debug

CLEAN_ALL:
	rm -rf radixHashJoin_basic
	rm -rf radixHashJoin_debug
