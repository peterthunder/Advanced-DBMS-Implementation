BASIC:
	@echo "Compile main...";
	gcc -g -o radixHashJoin_basic main.c supportFunctions.c radixHashJoin.c -lm

DEBUG:
	@echo "Compile main...";
	gcc -g -o radixHashJoin_debug main.c supportFunctions.c radixHashJoin.c -lm -D PRINTING