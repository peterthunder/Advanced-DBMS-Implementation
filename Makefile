BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -g -o radixHashJoin_basic main.c file_io.c parser.c radixHashJoin.c supportFunctions.c -lm

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -g -o radixHashJoin_debug main.c file_io.c parser.c radixHashJoin.c supportFunctions.c -lm -D PRINTING

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
