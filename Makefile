BASIC:
	@echo "Compile main...";
	gcc -g -o radixHashJoin_basic main.c supportFunctions.c radixHashJoin.c file_io.c -lm

DEBUG:
	@echo "Compile main...";
	gcc -g -o radixHashJoin_debug main.c supportFunctions.c radixHashJoin.c file_io.c -lm -D PRINTING