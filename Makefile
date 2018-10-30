make:
	@echo "Compile main...";
	gcc -g -o radixHashJoin main.c supportFunctions.c radixHashJoin.c -lm
