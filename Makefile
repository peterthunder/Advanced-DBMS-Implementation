make:
	@echo "Compile main...";
	gcc -g -o main main.c supportFunctions.c radixHashJoin.c -lm
