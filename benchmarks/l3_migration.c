// This file has been created by l3_benchmark_builder.sh and based on l3_benchmark.tpl.c

#ifndef __L3_MIGRATION
#define __L3_MIGRATION

#include "../aux.c"
#include "../omp.c"

/*
	For a 2-socket machine, we allocate memory on each node and then
		Step 0: 
			Cores of a socket access the L3 cachelines allocated on memory of its socket 
			and are already in L3 cache (home access, cached)
		Step 1: 
			Cores of a socket access the L3 cachelines allocated on another socket 
			so that each cache line is touched once (non-home access, forwarding cacheline from home to non-home)
		Step 2: 
			Cores of a socket repeat accessing the forwarded (and now-cached ) cache lines 
			that were originally  allocated on the another socket (non-home access, cached)
		Step 3:
			Cores of a socket access the cachelines that have been allocated on 
			the memory of its nodes  (home access, forwarding back cachelines to home)
*/

int test_l3_atomic_write_cacheline_migration(struct par_env* pe, const int type, double* res, int tries, unsigned long num_accesses)
{
	// Initial checks
		if(tries == 0)
			tries = 5;
		if(num_accesses == 0)
			num_accesses = 1e7;
		printf("\033[3;31mcheck_migration\033[0;37m, type: %u, tries: %u, num_accesses: %'lu\n", type, tries, num_accesses);
		assert(pe != NULL && type >=1 && type <= 4 && res != NULL);
		res[0] = res[1] = res[2] = res[3] = res[4] = 0.0;
		assert(type == 2);
		assert(pe->nodes_count == 2);
		
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
	{
		unsigned int seed = time(0);
		unsigned long mt = - get_nano_time();
		unsigned long total_s = 0;
		unsigned long total_accesses = 0;
		#pragma omp parallel reduction(+: total_s) reduction(+: total_accesses)
		{
			const unsigned long tid = (unsigned long)omp_get_thread_num();
			ttimes[tid] = -get_nano_time();

			unsigned int* array = (unsigned int*)threads_arrays[tid];
			unsigned long length = threads_arrays_sizes[tid] / sizeof(unsigned int);
			unsigned long s = 0;
			
			unsigned long start_index = (tid % (pe->threads_count / 2)) * (2 * length / pe->threads_count);
			unsigned long end_index = ((tid % (pe->threads_count / 2)) + 1) * (2 * length / pe->threads_count);

			for(unsigned long index = start_index; index < end_index ; index += 16)
			{
				s += __atomic_fetch_add(array + index, seed + index, __ATOMIC_RELAXED);
			}

			total_accesses  += (end_index - start_index)/16;
			
			ttimes[tid] += get_nano_time();

			total_s = s;
		}
		mt += get_nano_time();

		double imb = get_idle_percentage(mt, ttimes, pe->threads_count);
		printf("\033[1;35mStep: %d (warmup)\033[0;37m; Time: %'10.2f (ms); Load imbalance: %5.2f%%\n", -1, mt / 1e6, imb);
		
		double rt = (1000.0 * total_accesses) / mt;
		unsigned long avg = total_accesses/pe->threads_count;
		double real_throughput = rt * (100 / (100 - imb));
		printf("Total Accesses: %'lu\n", total_accesses);
		printf("Throughput: %'.1f MT/s\n\n", real_throughput);
	}

	// Memory accesses
		double total_st_dev_per = 0;
		double total_load_imbalance = 0.0;
		const int cpr = 1000; // checks per round
		for(int r = 0; r < tries; r++)
		{
			printf("\033[3;%umRound: %u\033[0;37m\n", 32 + r % 5, r);


			for(int step = 0; step < 4; step++)
			{
				// Reset papi
				#pragma omp parallel 
				{
					unsigned tid = omp_get_thread_num();
					papi_reset(pe->papi_args[tid]);
				}

				unsigned long total_accesses = 0;
				unsigned long mt = - get_nano_time();
				unsigned long total_s = 0;
				#pragma omp parallel reduction(+: total_accesses)  reduction(+: total_s)
				{
					const unsigned long tid = (unsigned long)omp_get_thread_num();
					ttimes[tid] = - get_nano_time();

					unsigned int* array = (unsigned int*)threads_arrays[tid];
					if(step == 1 || step == 2)
						array = (unsigned int*)threads_arrays[(tid + pe->threads_count / 2) % pe->threads_count ];
					
					unsigned long length = threads_arrays_sizes[tid] / sizeof(unsigned int);
					unsigned long ta = 0;
					unsigned long s = 0;
					
					unsigned long start_index = (tid % (pe->threads_count / 2)) * (2 * length / pe->threads_count);
					unsigned long end_index = ((tid % (pe->threads_count / 2)) + 1) * (2 * length / pe->threads_count);

					for(unsigned long index = start_index; index < end_index ; index += 16)
					{
						s += __atomic_fetch_add(array + index, 1U, __ATOMIC_RELAXED);
						// s += array[index];
						// array[index]++;
					}

					ta = (end_index - start_index)/16;
					
					ttimes[tid] += get_nano_time();

					thread_accesses[tid] = ta;  
					total_accesses = ta;
					total_s = s;
				}
				mt += get_nano_time();

				// Read papi
				#pragma omp parallel
				{
					unsigned tid = omp_get_thread_num();

					if(thread_papi_read(pe) != 0)
						printf("  Could not read papi. (tid: %u)\n", tid);
				}

				double imb = get_idle_percentage(mt, ttimes, pe->threads_count);
				printf("\033[1;35mStep: %u\033[0;37m; Time: %'10.2f (ms); Load imbalance: %5.2f%%\n", step, mt / 1e6, imb);
				total_load_imbalance += imb;
				
				double rt = (1000.0 * total_accesses) / mt;
				unsigned long avg = total_accesses/pe->threads_count;
				unsigned long dv = (unsigned long)get_standard_deviation(thread_accesses, pe->threads_count);
				printf("Accesses:\n  Total: %'lu\n  Avg. per thread: %'lu\n  Std. Dev.: %'lu (%.2f%%)\ns:%lu\n", 
					total_accesses, avg, dv, (100.0 * dv / avg), total_s
				);

				double real_throughput = rt * (100 / (100 - imb));
				res[step] += real_throughput;
				printf("Throughput: %'.1f MT/s", real_throughput);
				
				print_hw_events(pe, 1);
				reset_hw_events(pe);
			}

			printf("\n\n-----------------------\n");
		}

		res[0] /= tries;
		res[1] /= tries;
		res[2] /= tries;
		res[3] /= tries;

		printf("Throughput MT/s:\n  Step 0: %'.1f;\n  Step 1: %'.1f;\n  Step 2: %'.1f;\n  Step 3: %'.1f;\n\n", 
			res[0], res[1], res[2], res[3]);

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

#endif