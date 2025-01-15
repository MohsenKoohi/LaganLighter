// This template file is used to create similar functions with different names 
// and instructions for benchmarking L3 bandwidth
// Format: FUNC_NAME and BENCHMARK_INSTRUCTIONS separated by $ with a //$ at the beginning 
//$test_l3_atomic_write$__atomic_fetch_add(array + index, 1U, __ATOMIC_RELAXED);$
//$test_l3_write$array[index] += index;$
//$test_l3_read$sum += array[index];$

#include "../aux.c"
#include "../omp.c"

#ifndef SPLITMIX64_NEXT
	# define SPLITMIX64_NEXT
	// https://prng.di.unimi.it/splitmix64.c
	unsigned long splitmix64_next(unsigned long* x) {
		unsigned long z = (*x += 0x9e3779b97f4a7c15);
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
		return z ^ (z >> 31);
	}
#endif

/*
	This function measures the throughput of memory accesses when they are hit/missed in L3.
	Accesses are performed in a random order using XoShiRo256++ (https://prng.di.unimi.it/xoshiro256plusplus.c)
	The memory arrays are pre-accessed to make the page faults and to initialize with non-zero values. 
	Zero values may accelerate the execution.

	Note:
		The correctness of the results are based on the assumption that 
		the cores (and their threads) which share a L3 cache have consecutive IDs.
		Thread-pining of LaganLighter provides such a requirement. 
		If the function is used without LL, it is necessary to check the assignment of cores to caches.
	
	Args:
		`type`: 
			1:	
				Accesses are hit to the local L3 cache of the processing core 
			2:	
				Accesses are hit to the L3 cache(s) of the NUMA node of the processing core. 
			  It can be different from type `1` in two cases:
			  	- On CPUs such as 7702 EPYC with 4 NUMA nodes/socket & POWER8NVL, cores inside a NUMA node have their own L3 caches
					- On CPUS such as Sapphire Rapids Gold 6438Y+, more than one NUMA nodes share a single L3 cache 
			3: 	
				Accesses are hit to the total space of the L3 cache(s) on the same NUMA node or other NUMA nodes
				We have used `numa_alloc_interleaved()` in this type, however, if NUMA nodes host unequal size 
				of the array, then  L3 caches may experience unequal #accesses. This issue should be solved later.
			4:  
				All accesses are missed by L3 caches.
				For this type, we use a memory size of min(1024 * |total L3 caches|, free_mem) .

		`res`: 
			An double array of length 5 which, upon return of the function, will contain :
				res[0] = Min throughput in MT/s  (Mega Transactions per Second)
				res[1] = Avg throughput in MT/s  (Mega Transactions per Second)
				res[2] = Max throughput in MT/s  (Mega Transactions per Second)
				res[3] = Avg. st. deviation of threads accesses in percentage
				res[4] = Avg. load imbalance

		`tries`:
			Number of rounds. Note that the order of accesses are changed on each round.
		
		`num_accesses`:
			Number of accesses
*/
int FUNC_NAME(struct par_env* pe, const int type, double* res, int tries, unsigned long num_accesses)
{
	// Initial checks
		if(tries == 0)
			tries = 5;
		if(num_accesses == 0)
			num_accesses = 1e7;
		printf("\033[3;31mFUNC_NAME\033[0;37m, type: %u, tries: %u, num_accesses: %'lu\n", type, tries, num_accesses);
		assert(pe != NULL && type >=1 && type <= 4 && res != NULL);
		res[0] = res[1] = res[2] = res[3] = res[4] = 0.0;

		const unsigned int L3_caches_count = pe->L3_count;
		printf("L3_caches_count: %u\n", L3_caches_count);

	// Allocating memory 
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);
		double* thread_accesses = calloc(sizeof(double), pe->threads_count);
		assert(thread_accesses != NULL);

		char** allocated_memories = calloc(sizeof(char*), pe->threads_count);
		assert(allocated_memories != NULL);
		unsigned long* allocated_memory_sizes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(allocated_memory_sizes != NULL);
		unsigned int amc = 0;  // allocated_memories_count

		char** threads_arrays = calloc(sizeof(char*), pe->threads_count);
		unsigned long* threads_arrays_sizes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(threads_arrays != NULL);

		if(type == 1) // Hit by local L3 cache
		{
			unsigned int t = 0;
			for(unsigned int c = 0; c < L3_caches_count; c++)
			{
				allocated_memories[amc] = numa_alloc_onnode(4096 + pe->L3_cache_size, pe->thread2node[t]);
				assert(allocated_memories[amc] != NULL);
				allocated_memory_sizes[amc] = 4096 + pe->L3_cache_size;

				threads_arrays[t] = allocated_memories[amc];
				threads_arrays_sizes[t] = pe->L3_cache_size;
				
				// Alignment
				if((unsigned long)threads_arrays[t] % 4096 != 0)
					threads_arrays[t] = (char*)(
						(unsigned long)threads_arrays[t] + 4096 - (unsigned long)threads_arrays[t] % 4096
					);
				assert((unsigned long)threads_arrays[t] % 4096 == 0);

				// Setting the arrays of  threads  with this l3 cache
				for (int t0 = 1; t0 < pe->threads_count / L3_caches_count; t0++)
				{
					threads_arrays[t + t0] = threads_arrays[t];
					threads_arrays_sizes[t + t0] = threads_arrays_sizes[t];
				}

				t += pe->threads_count / L3_caches_count;
				amc++;
			}
		}
		else if(type == 2)  // Hit by L3 caches of the NUMA nodes
		{
			unsigned int t = 0;
			for(unsigned int n = 0; n < pe->nodes_count; n++)
			{
				double node_L3_caches_count = 1.0 * L3_caches_count * pe->node_threads_length[n] / pe->threads_count;

				allocated_memories[amc] = numa_alloc_onnode(4096 + pe->L3_cache_size * node_L3_caches_count , n);
				assert(allocated_memories[amc] != NULL);
				allocated_memory_sizes[amc] = 4096 + pe->L3_cache_size * node_L3_caches_count;

				threads_arrays[t] = allocated_memories[amc];
				threads_arrays_sizes[t] = pe->L3_cache_size * node_L3_caches_count;
				
				// Alignment
				if((unsigned long)threads_arrays[t] % 4096 != 0)
					threads_arrays[t] = (char*)(
						(unsigned long)threads_arrays[t] + 4096 - (unsigned long)threads_arrays[t] % 4096
					);
				assert((unsigned long)threads_arrays[t] % 4096 == 0);

				// Setting the arrays of  threads  with l3 caches in this node
				for (int t0 = 1; t0 < pe->node_threads_length[n]; t0++)
				{
					threads_arrays[t + t0] = threads_arrays[t];
					threads_arrays_sizes[t + t0] = threads_arrays_sizes[t];
				}

				t += pe->node_threads_length[n];
				amc++;
			}
		}
		else if(type == 3 || type == 4) // 3 = Hit by total L3 caches, 4 = Miss 
		{
			if(type == 3)
				allocated_memory_sizes[amc] = 4096 + pe->L3_cache_size * L3_caches_count;
			if(type == 4)
				allocated_memory_sizes[amc] = 4096 + min(pe->L3_cache_size * L3_caches_count * 1024UL, get_free_mem() - get_swap_size());
			allocated_memories[amc] = numa_alloc_interleaved(allocated_memory_sizes[amc]);
			assert(allocated_memories[amc] != NULL);
				
			threads_arrays[0] = allocated_memories[amc];
			threads_arrays_sizes[0] = allocated_memory_sizes[amc];

			// Alignment
			if((unsigned long)threads_arrays[0] % 4096 != 0)
				threads_arrays[0] = (char*)(
					(unsigned long)threads_arrays[0] + 4096 - (unsigned long)threads_arrays[0] % 4096
				);
			assert((unsigned long)threads_arrays[0] % 4096 == 0);

			for(unsigned int t = 0; t < pe->threads_count; t++)
			{
				threads_arrays[t] = threads_arrays[0];
				threads_arrays_sizes[t] = threads_arrays_sizes[0];
			}

			amc++;
		}
		else 
			assert(0 && "No valid type.");

	// Printing the allocated memory arrays
		printf("Allocated memories:");
		for(int t = 0; t < pe->threads_count; t++)
		{
			if(t == 0 || threads_arrays[t] != threads_arrays[t-1])
				printf("\nArray: %p; size: %'lu MB; Threads: %3u", threads_arrays[t], threads_arrays_sizes[t]/(1024*1024), t);
			else
				printf(", %3u", t);
		}
		printf("\n\n");
	
	// Making page faults and initializing 
		for(unsigned a = 0; a < amc; a++)
		{
			unsigned int* array = (unsigned int*)allocated_memories[a];
			unsigned int seed = time(0);
			#pragma omp parallel for
			for(unsigned long s = 0; s < allocated_memory_sizes[a] / 4; s++)
					array[s] = s + seed;
		}
		
	// Memory accesses
		double total_throughput = 0;
		double total_st_dev_per = 0;
		double total_load_imbalance = 0.0;
		const int cpr = 1000; // checks per round
		for(int r = 0; r < tries; r++)
		{
			printf("\033[3;%umRound: %u\033[0;37m\n", 31 + r % 6, r);

			// Reset papi
			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				papi_reset(pe->papi_args[tid]);
			}

			volatile unsigned long round_finished = 0;
			unsigned long total_accesses = 0;
			unsigned long total_sum = 0;
			unsigned long mt = - get_nano_time();
			#pragma omp parallel reduction(+: total_accesses) reduction(+: total_sum)
			{
				const unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();

				unsigned int* array = (unsigned int*)threads_arrays[tid];
				unsigned long length = threads_arrays_sizes[tid] / sizeof(unsigned int);

				unsigned long x = r * pe->threads_count + tid;
				unsigned long s0 = splitmix64_next(&x);
				unsigned long s1 = splitmix64_next(&x);
				unsigned long s2 = splitmix64_next(&x);
				unsigned long s3 = splitmix64_next(&x);
				
				unsigned long index = 0;
				unsigned long a0 = 0;
				unsigned long sum = 0;

				for(; a0 < num_accesses / cpr && !round_finished; a0++)
					for(unsigned long a1 = 0; a1 < cpr; a1++)
					{
						// https://prng.di.unimi.it/xoshiro256plusplus.c
						{
							unsigned long t = s0 + s3;
							index = ((t << 23) | (t >> (64 - 23))) + s0;

							t = s1 << 17;
							s2 ^= s0;
							s3 ^= s1;
							s1 ^= s2;
							s0 ^= s3;
							s2 ^= t;
							s3 = (s3 << 45) | (s3 >> (64 - 45));
						}

						index = index % length;

						BENCHMARK_INSTRUCTIONS
					}

				round_finished = 1;
				total_sum = sum;
				
				ttimes[tid] += get_nano_time();

				thread_accesses[tid] = a0 * cpr;  
				total_accesses += a0 * cpr;
			}
			mt += get_nano_time();
			printf("total_sum: %lu\n", total_sum);

			// Read papi
			#pragma omp parallel
			{
				unsigned tid = omp_get_thread_num();

				if(thread_papi_read(pe) != 0)
					printf("  Could not read papi. (tid: %u)\n", tid);
			}

			double imb = get_idle_percentage(mt, ttimes, pe->threads_count);
			printf("Time: %'10.2f (ms); Load imbalance: %5.2f%%\n", mt / 1e6, imb);
			total_load_imbalance += imb;
			
			double rt = (1000.0 * total_accesses) / mt;
			unsigned long avg = total_accesses/pe->threads_count;
			unsigned long dv = (unsigned long)get_standard_deviation(thread_accesses, pe->threads_count);
			printf("Accesses:\n  Total: %'lu\n  Avg. per thread: %'lu\n  Std. Dev.: %'lu (%.2f%%)\n", 
				total_accesses, avg, dv, (100.0 * dv / avg)
			);

			printf("Throughput: %'.1f MT/s", rt);
			total_throughput += rt;
			total_st_dev_per += (100.0 * dv / avg);
			if(r == 0)
				res[0] = res[2]  = rt;
			else if(rt < res[0])
				res[0] = rt;
			else if(rt > res[2])
				res[2] = rt;

			print_hw_events(pe, 1);
			reset_hw_events(pe);

			// printf("Thread exec times (in seconds): ");
			// for(int t = 0; t < pe->threads_count; t++)
			// 	printf("%.2f, ", t, ttimes[t]/1e9);
			// printf("\n");
			printf("\n");

		}

		res[1] = total_throughput / tries;
		res[3] = total_st_dev_per / tries;
		res[4] = total_load_imbalance / tries;

	// Releasing mem
		for(unsigned c = 0; c < amc; c++)
		{
			numa_free(allocated_memories[c], allocated_memory_sizes[c]);
			allocated_memories[c] = NULL;
		}
		free(allocated_memory_sizes);
		allocated_memory_sizes = NULL;
		free(allocated_memories);
		allocated_memories = NULL;

		free(threads_arrays);
		threads_arrays = NULL;		
		free(threads_arrays_sizes);
		threads_arrays_sizes = NULL;

		free(ttimes);
		ttimes = NULL;
		free(thread_accesses);
		thread_accesses = NULL;

	return 0;
}
