FLAGS := -O3 -std=c11 -lm $(extra_args) #-DDEBUG

%: %.c

apm-read: apm-read.c Makefile
	gcc apm-read.c $(FLAGS) -o apm-read.o 
	./apm-read.o

test: test.c Makefile
	gcc -fopenmp $(FLAGS) -fopenmp test.c -o test.o
	./test.o

clean:	
	rm -f *.o 

.SECONDARY: