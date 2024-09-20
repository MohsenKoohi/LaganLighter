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
	This array can be used as an RA_n2o, new to old reordering array (indexed by a new vertex ID to get its old vertex ID ) 
	to reorder the graph.
	
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
	This array can be used as an RA_n2o, new to old reordering array (indexed by a new vertex ID to get its old vertex ID ) 
	to reorder the graph.
	
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

/*
	A vertex relabelling arrary to randomize the graph.
	
	`call_back`, if defined,  is called at the end of each iteration.
*/
unsigned int* random_ordering(struct par_env* pe, unsigned int vertices_count, int iterations, void (*call_back)(unsigned int*, unsigned int))
{
	// (1.1) Initial checks
		unsigned long t0 = - get_nano_time();
		assert(pe != NULL);

		if(iterations == 0)
			iterations = 1;
		
		printf("\n\033[3;32mrandom_ordering\033[0;37m using \033[3;32m%d\033[0;37m threads.\n", pe->threads_count);

	// (1.2) Memory allocation
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		unsigned int* RA_o2n = numa_alloc_interleaved(sizeof(unsigned int) * vertices_count);
		unsigned int* it_o2n = numa_alloc_interleaved(sizeof(unsigned int) * vertices_count);
		unsigned int* vertex_selected = numa_alloc_interleaved(sizeof(unsigned int) * vertices_count);
		assert(RA_o2n != NULL && it_o2n != NULL && vertex_selected != NULL);

		#pragma omp parallel for
		for(unsigned int v = 0; v < vertices_count; v++)
		{
			it_o2n[v] = -1U;
			RA_o2n[v] = v;
			vertex_selected[v] = 0;
		}

	// (2) iterations
		for(unsigned int i = 0; i < iterations; i++)
		{
			unsigned long t1 = - get_nano_time();

			// (2.1) Creating the new random relabeling array in it_o2n
			#pragma omp parallel
			{
				unsigned int tid = omp_get_thread_num();

				unsigned int w_start = tid * (vertices_count / pe->threads_count);
				unsigned int w_end = (tid + 1) * (vertices_count / pe->threads_count);
				if(tid == pe->threads_count - 1)
					w_end = vertices_count;
				unsigned int w_length = w_end - w_start;
				unsigned int counter = 0;

				// Random seed Using splitmix64 (https://prng.di.unimi.it/splitmix64.c) to fill the seeds 
					unsigned long s[4];
					rand_initialize_splitmix64(s, pe->threads_count * (i + 1) * get_nano_time() + tid);

				// Iterating over vertices
				for(unsigned int v = 0; v < vertices_count; v++)
				{
					if(vertex_selected[v] == i + 1)
						continue;

					if(!__sync_bool_compare_and_swap(&vertex_selected[v], i, i + 1))
						continue;

					unsigned int index = w_start + rand_xoshiro256(s) % w_length;
					while(it_o2n[index] != -1U)
					{
						index++;
						if(index >= w_end)
							index = w_start;
					}

					it_o2n[index] = v;

					counter++;
					if(counter >= .9 * w_length)
						break;

					v += rand_xoshiro256(s) % pe->threads_count;
				}

				unsigned int last_index = w_start;

				for(unsigned int v = 0; v < vertices_count; v++)
				{
					if(vertex_selected[v] == i + 1)
						continue;

					if(!__sync_bool_compare_and_swap(&vertex_selected[v], i, i + 1))
						continue;

					while(it_o2n[last_index] != -1U)
					{
						last_index++;
						assert(last_index != w_end);
					}

					it_o2n[last_index] = v;

					counter++;
					if(counter == w_length)
						break;
				}
			}

			// #ifndef NDEBUG 
			// 	#pragma omp parallel for
			// 	for(unsigned int v = 0; v < vertices_count; v++)
			// 		assert(it_o2n[v] != -1U && vertex_selected[v] == i+1);

			// 	assert(1 == relabeling_array_validate(pe, it_o2n, vertices_count));
			// #endif

			// (2.2) Setting RA_o2n 
			#pragma omp parallel for
			for(unsigned int v = 0; v < vertices_count; v++)
			{
				unsigned int prev_map = RA_o2n[v];
				assert(it_o2n[prev_map] != -1U);
			
				RA_o2n[v] = it_o2n[prev_map];
				it_o2n[prev_map] = -1U;
			}

			t1 += get_nano_time();
			printf("  Iteration %u finished in %'.1f milliseconds.\n", i, t1 / 1e6);

			if(call_back)
				call_back(RA_o2n, i);
		}

	// Releasing mem
		numa_free(it_o2n, sizeof(unsigned int) * vertices_count);
		numa_free(vertex_selected, sizeof(unsigned int) * vertices_count);
		it_o2n = NULL;
		vertex_selected = NULL;

	// Finalizing
		t0 += get_nano_time();
		printf("Execution time: %'10.1f (ms)\n\n", t0/1e6);

	return RA_o2n;
}

/*
	relabel_graph() 

	flags: 
		bit 0 : TODO: validate results
		bit 1 : sort neighbour-list of the output  
*/

struct ll_400_graph* relabel_graph(struct par_env* pe, struct ll_400_graph* g, unsigned int* RA_o2n, unsigned int flags)
{
	// Initial checks
		unsigned long tt = - get_nano_time();
		assert(pe != NULL && g != NULL && RA_o2n != NULL);
		assert(relabeling_array_validate(pe, RA_o2n, g->vertices_count));
		printf("\n\033[3;35mrelabel_graph\033[0;37m using \033[3;35m%d\033[0;37m threads.\n", pe->threads_count);

	// Partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		printf("partitions: %'u \n", partitions_count);
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(g, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Allocating memory
		struct ll_400_graph* out_graph =calloc(sizeof(struct ll_400_graph),1);
		assert(out_graph != NULL);
		out_graph->vertices_count = g->vertices_count;
		out_graph->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + g->vertices_count));
		assert(out_graph->offsets_list != NULL);

		unsigned long* partitions_total_edges = calloc(sizeof(unsigned long), partitions_count);
		assert(partitions_total_edges != NULL);

		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// (1) Identifying degree of vertices in the out_graph
		unsigned long mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			#pragma omp for nowait 
			for(unsigned int p = 0; p<partitions_count; p++)
			{
				for(unsigned int v = partitions[p]; v < partitions[p + 1]; v++)
				{
					unsigned int degree = g->offsets_list[v + 1] - g->offsets_list[v];
					unsigned int new_v = RA_o2n[v];
					out_graph->offsets_list[new_v] = degree; 
				}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		PTIP("(1) Identifying degrees");
		
	// (2) Calculating sum of edges of each partition in partitions_total_edges
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			#pragma omp for nowait 
			for(unsigned int p = 0; p<partitions_count; p++)
			{
				unsigned long sum = 0;
				for(unsigned int v = partitions[p]; v < partitions[p + 1]; v++)
					sum += out_graph->offsets_list[v];
				partitions_total_edges[p] = sum;
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		PTIP("(2) Calculating sum");
	
	// Partial sum of partitions_total_edges
		{
			unsigned long sum = 0;
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned long temp = partitions_total_edges[p];
				partitions_total_edges[p] = sum;
				sum += temp;
			}
			out_graph->edges_count = sum;
			printf("%-20s \t\t\t %'10lu\n","New graph edges:", out_graph->edges_count);
			assert(out_graph->edges_count == g->edges_count);
		}
		out_graph->offsets_list[out_graph->vertices_count] = out_graph->edges_count;
		out_graph->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * out_graph->edges_count);
		assert(out_graph->edges_list != NULL);

	// (3) Updating the out_graph->offsets_list
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			#pragma omp for nowait 
			for(unsigned int p = 0; p<partitions_count; p++)
			{
				unsigned long current_offset = partitions_total_edges[p];
				for(unsigned int v = partitions[p]; v < partitions[p + 1]; v++)
				{
					unsigned long v_degree = out_graph->offsets_list[v];
					out_graph->offsets_list[v] = current_offset;
					current_offset += v_degree;
				}

				if(p + 1 < partitions_count)
					assert(current_offset == partitions_total_edges[p + 1]);
				else
					assert(current_offset == out_graph->edges_count);
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		PTIP("(3) Update offsets_list");	

	// out_graph partitioning
		unsigned int* out_partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(out_partitions != NULL);
		parallel_edge_partitioning(out_graph, out_partitions, partitions_count);
		
	// (4) Writing edges
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned int tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			unsigned int partition = -1U;
				
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned int new_v = RA_o2n[v];
					unsigned long new_e = out_graph->offsets_list[new_v];

					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
					{
						out_graph->edges_list[new_e] = RA_o2n[g->edges_list[e]];
						new_e++;
					}

					assert(new_e == out_graph->offsets_list[new_v+1]);
				}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("(4) Writing edges");

	// (5) Sorting
		if((flags & 2U))
		{	
			mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int partition = -1U;	
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
					for(unsigned int v = out_partitions[partition]; v < out_partitions[partition + 1]; v++)
					{
						unsigned int degree = out_graph->offsets_list[v+1] - out_graph->offsets_list[v];
						if(degree < 2)
							continue;
						quick_sort_uint(&out_graph->edges_list[out_graph->offsets_list[v]], 0, degree - 1);
					}
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("(5) Sorting");
		}

	// Releasing memory
		free(partitions);
		partitions = NULL;

		free(out_partitions);
		out_partitions = NULL;

		dynamic_partitioning_release(dp);
		dp = NULL;

		free(partitions_total_edges);
		partitions_total_edges = NULL;

		free(ttimes);
		ttimes = NULL;

	// Finalizing
		tt += get_nano_time();
		printf("%-20s \t\t\t %'.3f (s)\n\n","Total time:", tt/1e9);
		print_ll_400_graph(out_graph);
		
	return out_graph;
}

unsigned int* get_create_fixed_random_ordering(struct par_env* pe, char* graph_basename, unsigned int vertices_count, int iterations)
{
	// Creating the folder, if does not exist
	{
		struct stat st = {0};
		if (stat(LL_GRAPH_RA_BIN_FOLDER, &st) == -1)
			mkdir(LL_GRAPH_RA_BIN_FOLDER, 0700);
	}

	// Identifying the file
	char* file_name = malloc(PATH_MAX);
	assert(file_name != NULL);
	sprintf(file_name, "%s/%s_RND_%u.bin", LL_GRAPH_RA_BIN_FOLDER, graph_basename, iterations);
	unsigned long file_size = sizeof(unsigned int) * vertices_count;

	// Creating the file
	if(access(file_name, F_OK) != 0)
	{
		unsigned int* RA = random_ordering(pe, vertices_count, iterations, NULL);
		assert(RA != NULL);

		int fd=open(file_name, O_RDWR|O_CREAT|O_TRUNC, 0600);
		if(fd < 0)
		{
			printf("Can't open the file, %d, %s\n",errno, strerror(errno));
			return NULL;
		}

		if(ftruncate(fd, file_size)!=0)
		{
			printf("Can't truncate the file, %d, %s\n",errno, strerror(errno));
			return NULL;	
		}

		unsigned int* stored_RA = mmap(
			NULL
			, file_size
			, PROT_READ|PROT_WRITE
			, MAP_SHARED
			, fd
			, 0
		);
		if(stored_RA == MAP_FAILED)
		{
			printf("Can't mmap the file, %d, %s\n",errno, strerror(errno));
			return NULL;	
		}

		#pragma omp parallel for
		for(unsigned int v = 0; v < vertices_count; v++)
			stored_RA[v] = RA[v];

		int ret = msync(stored_RA, file_size, MS_SYNC);
		assert(ret == 0);

		munmap(stored_RA, file_size);

		close(fd);
		fd = -1;
	}

	// Loading the array
	int fd=open(file_name, O_RDONLY);
	assert(fd > 0);

	unsigned int* RA = mmap(
		NULL
		, file_size
		, PROT_READ 
		, MAP_SHARED
		, fd
		, 0
	);
	assert(RA != MAP_FAILED);

	close(fd);
	fd = -1;

	free(file_name);
	file_name = NULL;

	return RA;
}
		
#endif