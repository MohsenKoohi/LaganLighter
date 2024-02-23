#ifndef __RELABEL_C
#define __RELABEL_C

#include "aux.c"
#include "graph.c"
#include "trans.c"


int relabeling_array_validate(struct par_env* pe, unsigned int* RA, unsigned int vertices_count)
{
	int res= 1;
	unsigned char* counts= numa_alloc_interleaved(sizeof(unsigned char) * (vertices_count));
	assert(counts != NULL);
	
	#pragma omp parallel for 
	for(unsigned int v=0; v < vertices_count; v++)
	{
		unsigned char t = __atomic_fetch_add(&counts[RA[v]], 1U, __ATOMIC_RELAXED);
		if(t != 0)
		{
			printf("v: %'u \t RA[v]: %'u \t counts:%'u\n",v,RA[v], t);
			res = 0;
		}
		assert(t == 0);
	}

	numa_free(counts, sizeof(unsigned char) * (vertices_count));
	counts = NULL;

	return res;
}

/*
	SAPCO Sort: Structure-Aware Parallel Counting Sort

	https://blogs.qub.ac.uk/DIPSA/SAPCo-Sort-Optimizing-Degree-Ordering-for-Power-Law-Graphs

	@INPROCEEDINGS{ 10.1109/ISPASS55109.2022.00015,
		author={Koohi Esfahani, Mohsen and Kilpatrick, Peter and Vandierendonck, Hans},
		booktitle={2022 IEEE International Symposium on Performance Analysis of Systems and Software (ISPASS)}, 
		title={{SAPCo Sort}: Optimizing Degree-Ordering for Power-Law Graphs}, 
		year={2022},
		volume={},
		number={},
		pages={},
		publisher={IEEE Computer Society},
		doi={10.1109/ISPASS55109.2022.00015}
	}

	This function returns an array containing the vertices IDs with degrees in descending order. 
	This array is used as an old to new reordering array (RA_n2o) to reorder the graph.

	We introduce SAPCo Sort as a novel parallel count-sorting for skewed datasets. 
	It has 4 phases:
	(1) Initialization: Identifying max-degree, dividing the vertices into a number of partitions, and allocating a per-partition counter that is an array of MAX_LOW_DEGREE (e.g. 1000) integers. A global counter array is also allocated for counting high-degree vertices.
	(2) Theneach parallel threads pass over partitions: for low-degree vertices, the thread increases the related index in the counter of that partition. For high-degree vertices, threads increase atomically the related index in the global counter.
	(3) Calculating the offsets in the result array(that is returned by the function) for each degree/partition.
	(4) Writing vertices IDs by threads by performing another pass over all vertices and by using offsets calculated in step 3.

	flags:
		0: print details

	exec_info: if not NULL, will have 
		[0]: exec time
		[1-7]: papi events
		[8-11]: timing 
*/

unsigned int* sapco_sort_degree_ordering(struct par_env* pe, struct ll_400_graph* g, unsigned long* exec_info, unsigned int flags)
{
	// (1.1) Initial checks
		unsigned long t0 = - get_nano_time();
		assert(pe != NULL && g!= NULL && g->vertices_count != 0 && g->offsets_list != NULL);
		assert(g->vertices_count < (1UL<<32));
		if(flags & 1U)
			printf("\n\033[3;33msapco_sort_degree_ordering\033[0;37m using \033[3;33m%d\033[0;37m threads.\n", pe->threads_count);

		// Reset papi
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			papi_reset(pe->papi_args[tid]);
		}

	// (1.2) Identifying the max_degree
		unsigned long max_degree = 0;
		#pragma omp parallel for reduction(max: max_degree)
		for(unsigned int v = 0; v < g->vertices_count; v++)
		{
			unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
			if(degree > max_degree)
				max_degree = degree;
		}
		if(flags & 1U)
			printf("Max_degree: \t\t\t%'lu\n",max_degree);

		max_degree++;

	// (1.3) Memory allocation
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		unsigned int partitions_count = 64 * pe->threads_count;
		unsigned long MAX_LOW_DEGREE = min(1000, max_degree / 2 + 1);
		if(flags & 1U)
			printf("MAX_LOW_DEGREE: \t\t%'u\n",MAX_LOW_DEGREE);

		unsigned int* ret = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(ret != NULL);

		unsigned int* partitions_counters = numa_alloc_interleaved(sizeof(unsigned int) * (MAX_LOW_DEGREE * partitions_count + max_degree) );
		assert(partitions_counters != NULL);
		unsigned int* global_counter = &partitions_counters[MAX_LOW_DEGREE * partitions_count];

		unsigned int* offsets = calloc(sizeof(unsigned int), partitions_count);
		assert(offsets != NULL);

	// (2) Counting degrees for each partition
		unsigned long mt = - get_nano_time();
		if(exec_info)
			exec_info[8] = t0 - mt;

		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			#pragma omp for nowait
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned int* pc = &partitions_counters[p * MAX_LOW_DEGREE];

				unsigned int start_vertex = (g->vertices_count/partitions_count) * p;
				unsigned int end_vertex = (g->vertices_count/partitions_count) * (p + 1);
				if(p + 1 == partitions_count)
					end_vertex = g->vertices_count;

				for(unsigned int v = start_vertex; v < end_vertex; v++)
				{
					unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
					if(degree < MAX_LOW_DEGREE)
						pc[degree]++;
					else
						__atomic_fetch_add(&global_counter[degree], 1U, __ATOMIC_RELAXED);
				}
			}
			
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("(2) Counting degrees");
		if(exec_info)
			exec_info[9] = mt;
		
	// (3.1) Calculating total count of low-degree counts over different partitions
		mt = -get_nano_time();
		#pragma omp parallel for 
		for(unsigned int v=0; v<MAX_LOW_DEGREE; v++)
		{
			unsigned int sum = 0;
			
			for(unsigned int p = 0; p<partitions_count; p++)
				sum += partitions_counters[p * MAX_LOW_DEGREE + v];
			
			global_counter[v] = sum;
		}
			
	// (3.2) Calculating offsets for each degree
		#pragma omp parallel for 
		for(unsigned int p=0; p<partitions_count; p++)
		{
			unsigned int start_vertex = (max_degree/partitions_count) * p;
			unsigned int end_vertex = (max_degree/partitions_count) * (p + 1);
			if(p + 1 == partitions_count)
				end_vertex = max_degree;

			unsigned int sum = 0;
			
			for(unsigned int v = start_vertex; v<end_vertex; v++)
				sum += global_counter[v];
			
			offsets[p] = sum;
		}

		unsigned int total_offset = 0;
		for(int p = partitions_count - 1; p >= 0; p--)
		{
			unsigned int temp = offsets[p];
			offsets[p] = total_offset;
			total_offset += temp;
		}
		assert(total_offset == g->vertices_count);

		#pragma omp parallel for 
		for(unsigned int p=0; p<partitions_count; p++)
		{
			long start_vertex = (max_degree/partitions_count) * p;
			long end_vertex = (max_degree/partitions_count) * (p + 1);
			if(p + 1 == partitions_count)
				end_vertex = max_degree;

			unsigned int offset = offsets[p];
			for(long v = end_vertex - 1; v >= start_vertex; v--)
			{
				unsigned int temp = global_counter[v];
				global_counter[v] = offset;
				offset += temp;
			}
			
			if(p == 0)
				assert(offset == g->vertices_count);
			else
				assert(offset == offsets[p-1]);
		}

	// (3.3) Distributing offsets of each low-degree vertex to different partitions
		#pragma omp parallel for 
		for(unsigned int v=0; v < MAX_LOW_DEGREE; v++)
		{
			unsigned int offset = global_counter[v];
			
			for(unsigned int p = 0; p<partitions_count; p++)
			{
				unsigned int temp = partitions_counters[p * MAX_LOW_DEGREE + v];
				partitions_counters[p * MAX_LOW_DEGREE + v] = offset;
				offset += temp;
			}
			
			if(v == 0)
				assert(offset == g->vertices_count);
			else
				assert(offset == global_counter[v - 1]);
		}

		mt += get_nano_time();
		if(flags & 1U)
			PT("(3) Setting offsets");
		if(exec_info)
			exec_info[10] = mt;
		
	// (4) Writing IDs
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			#pragma omp for nowait
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned int* pc = &partitions_counters[p * MAX_LOW_DEGREE];

				unsigned int start_vertex = (g->vertices_count/partitions_count) * p;
				unsigned int end_vertex = (g->vertices_count/partitions_count) * (p + 1);
				if(p + 1 == partitions_count)
					end_vertex = g->vertices_count;

				for(unsigned int v = start_vertex; v < end_vertex; v++)
				{
					unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
					unsigned int offset;
					if(degree < MAX_LOW_DEGREE)
						offset = pc[degree]++;
					else
						offset = __atomic_fetch_add(&global_counter[degree], 1U, __ATOMIC_RELAXED);
					
					ret[offset] = v;
				}
			}
			
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("(4) Writing IDs");
		if(exec_info)
			exec_info[11] = mt;

	// Releasing memory
		free(ttimes);
		ttimes = NULL;

		numa_free(partitions_counters, sizeof(unsigned int) * (MAX_LOW_DEGREE * partitions_count + max_degree) );
		partitions_counters = NULL;
		global_counter = NULL;

		free(offsets);
		offsets = NULL;

	// Finalizing
		t0 += get_nano_time();
		if(flags & 1U)
			printf("\nExecution time: %'10.1f (ms)\n", t0/1e6);
		if(exec_info)
		{
			exec_info[0] = t0;

			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			
			if(flags & 1U)
				print_hw_events(pe, 1);

			copy_reset_hw_events(pe, &exec_info[1], 1);

			printf("\n");
		}

	return ret;
}

/*
	This function returns an array containing the vertices IDs with degrees in descending order. 
	This array can be used as an RA_n2o, old to new reordering array to reorder the graph.
	
	We use parallel counting sort:
		Step-1: Identifying max-degree
		Step-2: Counting degrees
		Step-3: Calculating offsets
		Step-4: Writing IDs
	flags:
		0: print details

	exec_info: if not NULL, will have 
		[0]: exec time
		[1-7]: papi events
		[8-11]: timings of steps
*/

unsigned int* counting_sort_degree_ordering(struct par_env* pe, struct ll_400_graph* g, unsigned long* exec_info, unsigned int flags)
{
	// (1.1) Initial checks
		unsigned long t0 = - get_nano_time();
		assert(pe != NULL && g!= NULL && g->vertices_count != 0 && g->offsets_list != NULL);
		assert(g->vertices_count < (1UL<<32));
		if(flags & 1U)
			printf("\n\033[3;33mcounting_sort_degree_ordering\033[0;37m using \033[3;33m%d\033[0;37m threads.\n", pe->threads_count);

		// Reset papi
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			papi_reset(pe->papi_args[tid]);
		}

	// (1.2) Identifying the max_degree
		unsigned long max_degree = 0;
		#pragma omp parallel for reduction(max: max_degree)
		for(unsigned int v = 0; v < g->vertices_count; v++)
		{
			unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
			if(degree > max_degree)
				max_degree = degree;
		}
		if(flags & 1U)
			printf("Max_degree: \t\t\t%'lu\n",max_degree);

		max_degree++;

	// (1.3) Memory allocation
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		unsigned int partitions_count = 64 * pe->threads_count;
		unsigned int* offsets = calloc(sizeof(unsigned int), partitions_count);
		assert(offsets != NULL);

		unsigned int* ret = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(ret != NULL);

		unsigned int** threads_counters = calloc(sizeof(unsigned int*), pe->threads_count);
		assert(threads_counters != NULL);
		
		unsigned int* global_counter = numa_alloc_interleaved(sizeof(unsigned int) * max_degree );
		assert(global_counter != NULL);
		
	// (2) Counting degrees
		unsigned long mt = - get_nano_time();
		if(exec_info)
			exec_info[8] = t0 - mt;

		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			threads_counters[tid] = numa_alloc_interleaved(sizeof(unsigned int) * max_degree );
			assert(threads_counters[tid] != NULL);
			unsigned int* my_counter = threads_counters[tid];

			#pragma omp for nowait
			for(unsigned int v = 0; v < g->vertices_count; v++)
			{
				unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
				my_counter[degree]++;
			}
			
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("(2) Counting degrees");
		if(exec_info)
			exec_info[9] = mt;
		
	// (3.1) Calculating total count of low-degree counts over different partitions
		mt = -get_nano_time();
		#pragma omp parallel for 
		for(unsigned int v=0; v < max_degree; v++)
		{
			unsigned int sum = 0;
			
			for(unsigned int t = 0; t<pe->threads_count; t++)
				sum += threads_counters[t][v];
			
			global_counter[v] = sum;
		}
			
	// (3.2) Calculating offsets for each degree
		#pragma omp parallel for 
		for(unsigned int p=0; p<partitions_count; p++)
		{
			unsigned int start_vertex = (max_degree/partitions_count) * p;
			unsigned int end_vertex = (max_degree/partitions_count) * (p + 1);
			if(p + 1 == partitions_count)
				end_vertex = max_degree;

			unsigned int sum = 0;
			
			for(unsigned int v = start_vertex; v<end_vertex; v++)
				sum += global_counter[v];
			
			offsets[p] = sum;
		}

		unsigned int total_offset = 0;
		for(int p = partitions_count - 1; p >= 0; p--)
		{
			unsigned int temp = offsets[p];
			offsets[p] = total_offset;
			total_offset += temp;
		}
		assert(total_offset == g->vertices_count);

		#pragma omp parallel for 
		for(unsigned int p=0; p<partitions_count; p++)
		{
			long start_vertex = (max_degree/partitions_count) * p;
			long end_vertex = (max_degree/partitions_count) * (p + 1);
			if(p + 1 == partitions_count)
				end_vertex = max_degree;

			unsigned int offset = offsets[p];
			for(long v = end_vertex - 1; v >= start_vertex; v--)
			{
				unsigned int temp = global_counter[v];
				global_counter[v] = offset;
				offset += temp;
			}
			
			if(p == 0)
				assert(offset == g->vertices_count);
			else
				assert(offset == offsets[p-1]);
		}

		mt += get_nano_time();
		if(flags & 1U)
			PT("(3) Setting offsets");
		if(exec_info)
			exec_info[10] = mt;
		
	// (4) Writing IDs
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			// Releasing memory
			numa_free(threads_counters[tid], sizeof(unsigned int) * max_degree );
			threads_counters[tid] = NULL;

			#pragma omp for nowait
			for(unsigned int v = 0; v < g->vertices_count; v++)
			{
				unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
				unsigned int offset = __atomic_fetch_add(&global_counter[degree], 1U, __ATOMIC_RELAXED);
					
				ret[offset] = v;
			}
			
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("(4) Writing IDs");
		if(exec_info)
			exec_info[11] = mt;

	// Releasing memory
		free(ttimes);
		ttimes = NULL;

		free(threads_counters);
		threads_counters = NULL;

		numa_free(global_counter, sizeof(unsigned int) * max_degree);
		global_counter = NULL;

		free(offsets);
		offsets = NULL;

	// Finalizing
		t0 += get_nano_time();
		if(flags & 1U)
			printf("\nExecution time: %'10.1f (ms)\n", t0/1e6);
		if(exec_info)
		{
			exec_info[0] = t0;

			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			
			if(flags & 1U)
				print_hw_events(pe, 1);

			copy_reset_hw_events(pe, &exec_info[1], 1);

			printf("\n");
		}

	return ret;
}

#endif
