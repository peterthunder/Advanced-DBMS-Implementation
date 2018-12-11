BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -ggdb -Wchkp -o radixHashJoin_basic src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/supportFunctions.c

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -ggdb -Wall -Wchkp -o radixHashJoin_debug src/main.c src/file_io.c src/parser.c src/query_functions.c src/radixHashJoin.c src/supportFunctions.c -D PRINTING

UNIT_TESTING:
	cd UnitTesting/Unity-master/Testing/MyTests && rake
	# Add more UnitTesting directories.

CLEAN_BASIC:
	rm -rf radixHashJoin_basic

CLEAN_DEBUG:
	rm -rf radixHashJoin_debug

CLEAN_ALL:
	rm -rf radixHashJoin_basic
	rm -rf radixHashJoin_debug
