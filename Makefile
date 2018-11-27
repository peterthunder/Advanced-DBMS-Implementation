BASIC:
	make CLEAN_BASIC
	@echo "Compile main...";
	gcc -g -o radixHashJoin_basic main.c radixHashJoin.c file_io.c supportFunctions.c parser.c  -lm

DEBUG:
	make CLEAN_DEBUG
	@echo "Compile main...";
	gcc -g -o radixHashJoin_debug main.c radixHashJoin.c file_io.c supportFunctions.c parser.c -lm -D PRINTING

UNIT_TESTING:
	cd UnitTesting/Unity-master/Testing/RadixHashJoin_Test && rake

CLEAN_BASIC:
	rm -rf radixHashJoin_basic

CLEAN_DEBUG:
	rm -rf radixHashJoin_debug

CLEAN_ALL:
	rm -rf radixHashJoin_basic
	rm -rf radixHashJoin_debug
