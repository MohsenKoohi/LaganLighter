#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>

int main(int argc, char** argv){
    system("./apm-read.o");
    printf("--------------------------------------------------\n");
    #pragma omp parallel
    {
    	unsigned i = omp_get_max_threads();
    	unsigned tid = omp_get_thread_num();   
    	double temp = 1;
	if(tid < i) {
	    //do something to keep the processor busy for 4-5 seconds and consume energy;
	    clock_t begin = clock();
	    for(int j=1; j<10000;  j++) { 
		for(int k=1; k<10000;  k++) { 
			for(int l=1; l<3;  l++) { 
				temp/=((double)j/(double)k)*(double)l;
			}
		}
	    }
	    clock_t end = clock();
	    double time = (double)(end - begin) / CLOCKS_PER_SEC;
	    printf("%d Time: %f\n", tid, time);
	}
    }
    printf("--------------------------------------------------\n");
    system("./apm-read.o");
    printf("--------------------------------------------------\n");
    return 0;
}
