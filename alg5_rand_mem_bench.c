#include "aux.c"
#include "graph.c"
#include "benchmarks/l3_rwa.c"
#include "benchmarks/l3_migration.c"

int main(int argc, char** args)
{	
	// Locale initialization
		setlocale(LC_NUMERIC, "");
		setbuf(stdout, NULL);
		setbuf(stderr, NULL);
		read_env_vars();

		struct par_env* pe = initialize_omp_par_env();
		
	// Measuring accesses
		double res[60] = {0.0};
		// char* names [] = {"check_migration"};
		char* names [] = {"atomic_write","write","read"};
		unsigned long num_accesses = 1e7;



		for(int k = 0; k < sizeof(names)/sizeof(char*); k++)
		{
			printf("-------------------------------------------------------------------\n");
			printf("-------------------------------------------------------------------\n");
			printf("-------------------------------------------------------------------\n");
			printf("\033[1;33m%s\033[0;37m\n\n", names[k]);

			// for(int t = 1; t < 2; t++)
			for(int t = 0; t < 4; t++)
			{
				int ret;
				if(k == 0)
					// ret = test_l3_atomic_write_cacheline_migration(pe, t + 1, res + 20 * k + 5 * t , 5, num_accesses);
					ret = test_l3_atomic_write(pe, t + 1, res + 20 * k + 5 * t , 5, num_accesses);
				else if(k == 1)
					ret = test_l3_write(pe, t + 1, res + 20 * k + 5 * t , 5, num_accesses);
				else if(k == 2)
					ret = test_l3_read(pe, t + 1, res + 20 * k + 5 * t , 5, num_accesses);
				assert(ret == 0);
				printf("Min: %'.1f; Avg: %'.1f; Max: %'.1f; Std. Dev: %'.1f%%;  Avg. Load Imbalance: %'.2f%%;\n\n", 
					res[ 20 * k + 5 * t ], 
					res[20 * k + 5 * t  + 1], 
					res[20 * k + 5 * t  + 2] , 
					res[20 * k + 5 * t  + 3], 
					res[20 * k + 5 * t  + 4]
				);
				printf("----------------------------------------\n");
			}
		}
		printf("\n\n\n\n");

	// Printing results
		for(int k = 0; k < sizeof(names)/sizeof(char*); k++)
		{
			printf("------------------------\n");
			printf("\033[1;33m%s\033[0;37m\n", names[k]);

			printf("Type; Throughput (MT/s); Access Std. Dev %%; Load Imbalance %%;\n");
			for(int t = 0; t < 4; t++)
				printf("%u   ; %'17.1f;             %'5.1f;            %'5.2f;\n",
					t + 1, res[20 * k + 5 * t  + 1], res[20 * k + 5 * t  + 3], res[20 * k + 5 * t  + 4]
				);
		}

}