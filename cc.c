#ifndef __CC_C
#define __CC_C

// This file contains implementation of the Connected Components algorithms

/*

CC with pull direction in all iterations. We use this for validation of results.

flags: 
	bit 0: print stats
	bit 1: do not reset papi

exec_info: if not NULL, will have 
	[0]: exec time
	[1-7]: papi events
*/
unsigned int* cc_pull(struct par_env* pe, struct ll_400_graph* g, unsigned int flags, unsigned long* exec_info, unsigned int* ccs_p)
{
	// Initial checks
		assert(pe != NULL && g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;31mcc_pull\033[0;37m\n");

	// Reset papi
		if(!(flags & 2U))
			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				papi_reset(pe->papi_args[tid]);
			}

	// Allocate memory
		unsigned int* cc = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(cc != NULL);
		#pragma omp parallel for
		for(unsigned int v = 0; v < g->vertices_count; v++)
			cc[v] = v;

		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// Edge partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(g, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Pull iterations
		unsigned int cc_changed = 0;
		unsigned int cc_iter = 0;
		do
		{
			cc_changed = 0;
			unsigned long mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned long thread_cc_changed = 0;
				unsigned int partition = -1U;		
				
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
					for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					{
						unsigned int component = cc[v];
						for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
						{
							unsigned int neighbour = g->edges_list[e];
							if(cc[neighbour] < component)
								component = cc[neighbour];
						}

						if(component < cc[v])
						{
							cc[v] = component;
							thread_cc_changed++;
						}
					}
				}

				if(thread_cc_changed)
					__sync_fetch_and_add(&cc_changed, thread_cc_changed, __ATOMIC_SEQ_CST);
				
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			if(flags & 1U)
			{
				char temp[255];
				if(cc_changed < 100)
					sprintf(temp, "Iter: %'3u, |F|: %5u, time:", cc_iter, cc_changed);
				else
					sprintf(temp, "Iter: %'3u, |F|: %'4.1f%, time:", cc_iter, 100.0*cc_changed/g->vertices_count);
				PTIP(temp);
			}
			cc_iter++;
		}while(cc_changed);

	// Saving events
		if(!(flags & 2U))
		{
			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			if(flags & 1U)
				print_hw_events(pe, 1);
			if(exec_info)
				copy_reset_hw_events(pe, &exec_info[1], 1);
		}

	// Number of components
		if(ccs_p)
		{
			unsigned int ccs = 0;
			#pragma omp parallel for reduction(+:ccs)
			for(unsigned int v = 0; v < g->vertices_count; v++)
				if(cc[v] == v)
					ccs++;
			printf("|CCs|:            \t\t%'u\n",ccs);
			*ccs_p = ccs;
		}
		
	// Finalizing
		t0 += get_nano_time();
		printf("Total exec. time: \t\t %'.1f (ms)\n\n",t0/1e6);
		if(exec_info)
			exec_info[0] = t0;

	// Releasing memory
		free(partitions);
		partitions = NULL;

		free(ttimes);
		ttimes = NULL;

	return cc;
}

/*
	Thrifty Label Propagation Connected Components

	https://blogs.qub.ac.uk/DIPSA/Thrifty-Label-Propagation-Fast-Connected-Components-for-Skewed-Degree-Graphs/

	@INPROCEEDINGS{10.1109/Cluster48925.2021.00042,
	  author={Koohi Esfahani, Mohsen and Kilpatrick, Peter and Vandierendonck, Hans},
	  booktitle={2021 IEEE International Conference on Cluster Computing (CLUSTER)}, 
	  title={Thrifty Label Propagation: Fast Connected Components for Skewed-Degree Graphs}, 
	  year={2021},
	  volume={},
	  number={},
	  pages={226-237},
	  publisher={IEEE Computer Society},
	  doi={10.1109/Cluster48925.2021.00042}
	}

	flags:
		bit 0: print stats
		bit 1: do not reset papi

	exec_info: if not NULL, will have
		[0]: exec time
		[1-7]: papi events
		[8]: push max-degree
*/
unsigned int* cc_thrifty_400(struct par_env* pe, struct ll_400_graph* g, unsigned int flags, unsigned long* exec_info, unsigned int* ccs_p)
{
	// Initial checks
		assert(pe != NULL && g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;31mcc_thrifty\033[0;37m\n");

	// Reset papi
		if(!(flags & 2U))
			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				papi_reset(pe->papi_args[tid]);
			}
	
	// Allocate memory
		unsigned int* cc = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(cc != NULL);
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// Edge partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(g, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Zero Planting: Assigning the zero label to the vertex with max degree
		unsigned long mt = - get_nano_time();
		unsigned int max_degree_id = 0;
		{
			unsigned int max_vals[2] = {0,0};

			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int thread_max_vals[2] = {0,0};
				
				#pragma omp for nowait
				for(unsigned int v = 0; v < g->vertices_count; v++)
				{
					cc[v] = v + 1;

					unsigned int degree = g->offsets_list[v+1] - g->offsets_list[v];
					if(degree > thread_max_vals[0])
					{
						thread_max_vals[0] = degree;
						thread_max_vals[1] = v;
					}
				}

				// Update max_vals
				while(1)
				{
					unsigned long prev_val = *(unsigned long*)max_vals;
					if((unsigned int)prev_val >= thread_max_vals[0])
						break;
					__sync_val_compare_and_swap((unsigned long*)max_vals, prev_val, *(unsigned long*)thread_max_vals);
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
				PTIP("Zero Planting:");
			printf("Max. degree: \t %'u \t\t (ID: %'u)\n", max_vals[0], max_vals[1]);
			// Plant the zero label
			cc[max_vals[1]] = 0;
			max_degree_id = max_vals[1];
		}

	// Initial Push: Propagate the zero label to the neighbours of the max-degree vertex
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			#pragma omp for nowait
			for(unsigned long e = g->offsets_list[max_degree_id]; e < g->offsets_list[max_degree_id + 1]; e++)
				cc[g->edges_list[e]] = 0;

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("Initial Push:");

	// Pull iterations with Zero Convergence: 
		// If a vertex has reached zero label, its label cannot be reduced => do not process it.
		double frontier_density;
		unsigned int next_vertices;
		unsigned int cc_iter = 0;
		do
		{
			unsigned long next_edges = 0;
			next_vertices = 0;
	
			unsigned long mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int thread_next_vertices = 0;
				unsigned long thread_next_edges = 0;
				unsigned int partition = -1U;		
				
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
					for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					{
						unsigned int component = cc[v];
						// Zero Convergence
						if(!component)
							continue;

						for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
						{
							unsigned int neighbour = g->edges_list[e];
							if(cc[neighbour] < component)
							{
								component = cc[neighbour];
								// Zero Convergence
								if(!component)
									break;
							}
						}

						if(component < cc[v])
						{
							cc[v] = component;
							thread_next_vertices++;
							thread_next_edges += g->offsets_list[v+1] - g->offsets_list[v];
						}
					}
				}

				__sync_fetch_and_add(&next_vertices, thread_next_vertices, __ATOMIC_SEQ_CST);
				__sync_fetch_and_add(&next_edges, thread_next_edges, __ATOMIC_SEQ_CST);
				
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);

			frontier_density = 1.0 * (next_vertices + next_edges) / g->edges_count;
			if(flags & 1U)
			{
				char temp[255];
				if(next_vertices < 100)
					sprintf(temp, "Pull %'3u, |F|: %5u, Dns: %'5.2f, time:", cc_iter, next_vertices, frontier_density);
				else
					sprintf(temp, "Pull %'3u, |F|: %'4.1f%, Dns: %'5.2f, time:", cc_iter, 100.0 * next_vertices/g->vertices_count, frontier_density);
				PTIP(temp);
			}
			cc_iter++;
		}while(frontier_density >= 0.01);

	// Allocating memory for the shared worklists
		/*
			We implement worklists as a shared SPF (Sequentially Partially Filled) array between threads. 
			Since push iterations are sparse, we dedicate a chunk (with a size of cacheline, i.e., 16 uints) to each thread and after filling it we allocate another chunk. This way, we do not need to allocate per thread worklist. 
			
			The another point is about tiling. The initial implementation of thrifty used the 
			edge-tiling [Galois, DOI:10.1145/2517349.2522739] in the push iterations to allow
			concurrent processing of blocks of edges of vertices with large degrees. However, in the 
			push direction, we do not expect to see very high degree vertices and we do not use edge-tiling in this implementation. 

			Although, it is possible to add edge-tiling using the current data structure of worklist. To that target, we can perform edge-tiling before submitting vertices to the worklist: we can check degree of vertex and if it can be divided, we write multiple entries in the worklist, one for each tile. 
			In that case, We will need 3 `unsigned long`s per each tile: (vertex_id, start_neighbour_offset, end_neighbour_offset). 
			
			The `df` and `next_df` are used as byte array frontiers to identify if a vertex has been 
			previously stored in the worklist. We do not use atomics for accessing `df` as it is correct to process a 
			vertex multiple times in a CC iteration. 
		*/

		unsigned int waspr = 16;  // worklist_allocation_size_per_request
		unsigned int worklist_size = max(2 * next_vertices + waspr * pe->threads_count, 1024U * 1024);
		
		unsigned int* worklist = numa_alloc_interleaved(sizeof(unsigned int) * worklist_size);
		unsigned int worklist_length = waspr * pe->threads_count;				 // initial allocation per thread
		
		unsigned int* next_worklist = numa_alloc_interleaved(sizeof(unsigned int) * worklist_size);
		unsigned int next_worklist_length = waspr * pe->threads_count;    // initial allocation per thread

		unsigned char* df = numa_alloc_interleaved(sizeof(unsigned char) * g->vertices_count);
		unsigned char* next_df = numa_alloc_interleaved(sizeof(unsigned char) * g->vertices_count);
		assert(worklist != NULL && next_worklist != NULL && df != NULL && next_df != NULL);

	// Pull-Frontier: One more pull iteration to store active vertices into worklist
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			unsigned int partition = -1U;		

			unsigned int thread_worklist_index = tid * waspr;
			unsigned int thread_worklist_end = (tid + 1) * waspr;
			
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned int component = cc[v];
					// Zero Convergence
					if(!component)
						continue;

					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
					{
						unsigned int neighbour = g->edges_list[e];
						if(cc[neighbour] < component)
						{
							component = cc[neighbour];
							// Zero Convergence
							if(!component)
								break;
						}
					}

					// if new label has been found
					if(component < cc[v])
					{
						cc[v] = component;

						// set the frontier
						if(df[v])
							continue;

						// add to worklist
						df[v] = 1;
						worklist[thread_worklist_index++] = v;
						if(thread_worklist_index == thread_worklist_end)
						{
							// grab a new chunk
							do
							{
								thread_worklist_index = worklist_length;
								thread_worklist_end = thread_worklist_index + waspr;
							}while(__sync_val_compare_and_swap(&worklist_length, thread_worklist_index, thread_worklist_end) != thread_worklist_index);
							assert(worklist_length <= worklist_size);
						}
					}
				}
			}

			// fill unused indecis with -1U to prevent from being processed in the next iteration
			while(thread_worklist_index < thread_worklist_end)
				worklist[thread_worklist_index++] = -1U;

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);

		if(flags & 1U)
		{
			char temp[255];
			sprintf(temp, "Pull-Frontier, |F|: %'u, time:", worklist_length);
			PTIP(temp);
		}
		cc_iter++;

	// Push iterations
		unsigned int push_max_degree = 0;
		do
		{
			mt = - get_nano_time();
			next_vertices = 0;

			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
			
				unsigned int thread_next_worklist_index = tid * waspr;
				unsigned int thread_next_worklist_end = (tid + 1) * waspr;
				unsigned int thread_next_vertices = 0;

				#pragma omp for nowait reduction(max:push_max_degree)
				for(unsigned int index = 0; index < worklist_length; index++)
				{
					if(worklist[index] == -1U)
						continue;

					unsigned int v = worklist[index];
					if(df[v] == 0)
						continue;
					df[v] = 0;

					unsigned int degree = g->offsets_list[v+1] - g->offsets_list[v];
					if(degree > push_max_degree)
						push_max_degree = degree;

					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v+1]; e++)
					{
						unsigned int neighbour = g->edges_list[e];
						unsigned int changed = 0;

						while(1)
						{
							unsigned int cc_neighbour = cc[neighbour];
							unsigned int cc_v = cc[v];
							if(cc_neighbour <= cc_v)
								break;

							unsigned int prev_val = __sync_val_compare_and_swap(&cc[neighbour], cc_neighbour, cc_v);
							if(prev_val == cc_neighbour)
							{
								changed = 1;
								break;
							}
						}

						if(!changed)
							continue;

						if(next_df[neighbour])
							continue;

						next_df[neighbour] = 1;
						thread_next_vertices++;
						next_worklist[thread_next_worklist_index++] = neighbour;
						if(thread_next_worklist_index == thread_next_worklist_end)
						{
							// grab a new chunk
							do
							{
								thread_next_worklist_index = next_worklist_length;
								thread_next_worklist_end = thread_next_worklist_index + waspr;
							}while(__sync_val_compare_and_swap(&next_worklist_length, thread_next_worklist_index, thread_next_worklist_end) != thread_next_worklist_index);
							assert(next_worklist_length <= worklist_size);
						}
					}
				}	

				__sync_fetch_and_add(&next_vertices, thread_next_vertices, __ATOMIC_SEQ_CST);

				// fill unused indecis with -1U to prevent from being processed in the next iteration
				while(thread_next_worklist_index < thread_next_worklist_end)
					next_worklist[thread_next_worklist_index++] = -1U;

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
			{
				char temp[255];
				sprintf(temp, "Push, |F|: %5u, time:", next_vertices);
				PTIP(temp);
			}
			cc_iter++;

			// swapping
				{
					unsigned int* temp = worklist;
					worklist = next_worklist;
					next_worklist = temp;

					worklist_length = next_worklist_length;
					next_worklist_length = waspr * pe->threads_count;

					unsigned char* temp2 = df;
					df = next_df;
					next_df = temp2;
				}
		}while(next_vertices);
		if(flags & 1U)
			printf("Max-degree in push iterations: \t\t%'u\n", push_max_degree);
		if(exec_info)
			exec_info[8] = push_max_degree;

	// Saving events
		if(!(flags & 2U))
		{
			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			if(flags & 1U)
				print_hw_events(pe, 1);
			if(exec_info)
				copy_reset_hw_events(pe, &exec_info[1], 1);
		}

	// Counting number of components
		if(ccs_p)
		{
			unsigned int ccs = 1;
			#pragma omp parallel for reduction(+:ccs)
			for(unsigned int v = 0; v < g->vertices_count; v++)
				if(cc[v] == v + 1)
					ccs++;
			printf("|CCs|:            \t\t%'u\n",ccs);
			*ccs_p = ccs;
		}
		
	// Finalizing
		t0 += get_nano_time();
		printf("Total exec. time: \t\t %'.1f (ms)\n\n",t0/1e6);
		if(exec_info)
			exec_info[0] = t0;

	// Releasing memory
		free(partitions);
		partitions = NULL;
		free(ttimes);
		ttimes = NULL;
		numa_free(worklist, sizeof(unsigned int) * worklist_size);
		worklist = NULL;
		numa_free(next_worklist, sizeof(unsigned int) * worklist_size);
		next_worklist = NULL;
		numa_free(df, sizeof(unsigned char) * g->vertices_count);
		df = NULL;
		numa_free(next_df, sizeof(unsigned char) * g->vertices_count);
		next_df = NULL;

	return cc;
}

/*
	It is the thrifty for weighted graphs
*/
unsigned int* cc_thrifty_404(struct par_env* pe, struct ll_404_graph* g, unsigned int flags, unsigned long* exec_info, unsigned int* ccs_p)
{
	// Initial checks
		assert(pe != NULL && g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;31mcc_thrifty_w4\033[0;37m\n");

	// Reset papi
		if(!(flags & 2U))
			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				papi_reset(pe->papi_args[tid]);
			}
	
	// Allocate memory
		unsigned int* cc = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(cc != NULL);
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// Edge partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning((struct ll_400_graph*)g, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Zero Planting: Assigning the zero label to the vertex with max degree
		unsigned long mt = - get_nano_time();
		unsigned int max_degree_id = 0;
		{
			unsigned int max_vals[2] = {0,0};

			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int thread_max_vals[2] = {0,0};
				
				#pragma omp for nowait
				for(unsigned int v = 0; v < g->vertices_count; v++)
				{
					cc[v] = v + 1;

					unsigned int degree = g->offsets_list[v+1] - g->offsets_list[v];
					if(degree > thread_max_vals[0])
					{
						thread_max_vals[0] = degree;
						thread_max_vals[1] = v;
					}
				}

				// Update max_vals
				while(1)
				{
					unsigned long prev_val = *(unsigned long*)max_vals;
					if((unsigned int)prev_val >= thread_max_vals[0])
						break;
					__sync_val_compare_and_swap((unsigned long*)max_vals, prev_val, *(unsigned long*)thread_max_vals);
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
				PTIP("Zero Planting:");
			printf("Max. degree: \t %'u \t\t (ID: %'u)\n", max_vals[0], max_vals[1]);
			// Plant the zero label
			cc[max_vals[1]] = 0;
			max_degree_id = max_vals[1];
		}

	// Initial Push: Propagate the zero label to the neighbours of the max-degree vertex
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			#pragma omp for nowait
			for(unsigned long e = 2 * g->offsets_list[max_degree_id]; e < 2 * g->offsets_list[max_degree_id + 1]; e+=2)
				cc[g->edges_list[e]] = 0;

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("Initial Push:");

	// Pull iterations with Zero Convergence: 
		// If a vertex has reached zero label, its label cannot be reduced => do not process it.
		double frontier_density;
		unsigned int next_vertices;
		unsigned int cc_iter = 0;
		do
		{
			unsigned long next_edges = 0;
			next_vertices = 0;
	
			unsigned long mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int thread_next_vertices = 0;
				unsigned long thread_next_edges = 0;
				unsigned int partition = -1U;		
				
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
					for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					{
						unsigned int component = cc[v];
						// Zero Convergence
						if(!component)
							continue;

						for(unsigned long e = 2 * g->offsets_list[v]; e < 2 * g->offsets_list[v + 1]; e += 2)
						{
							unsigned int neighbour = g->edges_list[e];
							if(cc[neighbour] < component)
							{
								component = cc[neighbour];
								// Zero Convergence
								if(!component)
									break;
							}
						}

						if(component < cc[v])
						{
							cc[v] = component;
							thread_next_vertices++;
							thread_next_edges += g->offsets_list[v+1] - g->offsets_list[v];
						}
					}
				}

				__sync_fetch_and_add(&next_vertices, thread_next_vertices, __ATOMIC_SEQ_CST);
				__sync_fetch_and_add(&next_edges, thread_next_edges, __ATOMIC_SEQ_CST);
				
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);

			frontier_density = 1.0 * (next_vertices + next_edges) / g->edges_count;
			if(flags & 1U)
			{
				char temp[255];
				if(next_vertices < 100)
					sprintf(temp, "Pull %'3u, |F|: %5u, Dns: %'5.2f, time:", cc_iter, next_vertices, frontier_density);
				else
					sprintf(temp, "Pull %'3u, |F|: %'4.1f%, Dns: %'5.2f, time:", cc_iter, 100.0 * next_vertices/g->vertices_count, frontier_density);
				PTIP(temp);
			}
			cc_iter++;
		}while(frontier_density >= 0.01);

	// Allocating memory for the shared worklists
		/*
			We implement worklists as a shared SPF (Sequentially Partially Filled) array between threads. 
			Since push iterations are sparse, we dedicate a chunk of `waspr` size to each thread and 
			after filling it we allocate another chunk. This way, we do not need to alocate per vertex worklist.
			
			The another point is about tiling. The initial implementation of thrifty had edge-tiling to allow
			concurrent processing of blocks of edges of vertices with large degrees. However, in the 
			push direction, we do not expect to see very high degree vertices. 

			Although, it is possible to add edge-tiling using the current data structure of worklist. To that target, we can perform edge-tiling before submitting vertices to the worklist: we can check degree of vertex and if it can be divided, we write multiple entries in the worklist, one for each tile. 
			In that case, We will need 3 `unsigned long`s per each tile: (vertex_id, start_neighbour_offset, end_neighbour_offset). 
			
			The `df` and `next_df` are used as byte array frontiers to identify if a vertex has been 
			previously stored in the worklist. We do not use atomics for accessing df as it is correct to process a 
			vertex multiple times in a CC iteration. 
		*/

		unsigned int waspr = 16;  // worklist_allocation_size_per_request
		unsigned int worklist_size = max(2 * next_vertices + waspr * pe->threads_count, 1024U * 1024);
		
		unsigned int* worklist = numa_alloc_interleaved(sizeof(unsigned int) * worklist_size);
		unsigned int worklist_length = waspr * pe->threads_count;				 // initial allocation per thread
		
		unsigned int* next_worklist = numa_alloc_interleaved(sizeof(unsigned int) * worklist_size);
		unsigned int next_worklist_length = waspr * pe->threads_count;    // initial allocation per thread

		unsigned char* df = numa_alloc_interleaved(sizeof(unsigned char) * g->vertices_count);
		unsigned char* next_df = numa_alloc_interleaved(sizeof(unsigned char) * g->vertices_count);
		assert(worklist != NULL && next_worklist != NULL && df != NULL && next_df != NULL);

	// Pull-Frontier: One more pull iteration to store active vertices into worklist
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			unsigned int partition = -1U;		

			unsigned int thread_worklist_index = tid * waspr;
			unsigned int thread_worklist_end = (tid + 1) * waspr;
			
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned int component = cc[v];
					// Zero Convergence
					if(!component)
						continue;

					for(unsigned long e = 2 * g->offsets_list[v]; e < 2 * g->offsets_list[v + 1]; e += 2)
					{
						unsigned int neighbour = g->edges_list[e];
						if(cc[neighbour] < component)
						{
							component = cc[neighbour];
							// Zero Convergence
							if(!component)
								break;
						}
					}

					// if new label has been found
					if(component < cc[v])
					{
						cc[v] = component;

						// set the frontier
						if(df[v])
							continue;

						// add to worklist
						df[v] = 1;
						worklist[thread_worklist_index++] = v;
						if(thread_worklist_index == thread_worklist_end)
						{
							// grab a new chunk
							do
							{
								thread_worklist_index = worklist_length;
								thread_worklist_end = thread_worklist_index + waspr;
							}while(__sync_val_compare_and_swap(&worklist_length, thread_worklist_index, thread_worklist_end) != thread_worklist_index);
							assert(worklist_length <= worklist_size);
						}
					}
				}
			}

			// fill unused indecis with -1U to prevent from being processed in the next iteration
			while(thread_worklist_index < thread_worklist_end)
				worklist[thread_worklist_index++] = -1U;

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);

		if(flags & 1U)
		{
			char temp[255];
			sprintf(temp, "Pull-Frontier, |F|: %'u, time:", worklist_length);
			PTIP(temp);
		}
		cc_iter++;

	// Push iterations
		unsigned int push_max_degree = 0;
		do
		{
			mt = - get_nano_time();
			next_vertices = 0;

			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
			
				unsigned int thread_next_worklist_index = tid * waspr;
				unsigned int thread_next_worklist_end = (tid + 1) * waspr;
				unsigned int thread_next_vertices = 0;

				#pragma omp for nowait reduction(max:push_max_degree)
				for(unsigned int index = 0; index < worklist_length; index++)
				{
					if(worklist[index] == -1U)
						continue;

					unsigned int v = worklist[index];
					if(df[v] == 0)
						continue;
					df[v] = 0;

					unsigned int degree = g->offsets_list[v+1] - g->offsets_list[v];
					if(degree > push_max_degree)
						push_max_degree = degree;

					for(unsigned long e = 2 * g->offsets_list[v]; e < 2 * g->offsets_list[v+1]; e += 2)
					{
						unsigned int neighbour = g->edges_list[e];
						unsigned int changed = 0;

						while(1)
						{
							unsigned int cc_neighbour = cc[neighbour];
							unsigned int cc_v = cc[v];
							if(cc_neighbour <= cc_v)
								break;

							unsigned int prev_val = __sync_val_compare_and_swap(&cc[neighbour], cc_neighbour, cc_v);
							if(prev_val == cc_neighbour)
							{
								changed = 1;
								break;
							}
						}

						if(!changed)
							continue;

						if(next_df[neighbour])
							continue;

						next_df[neighbour] = 1;
						thread_next_vertices++;
						next_worklist[thread_next_worklist_index++] = neighbour;
						if(thread_next_worklist_index == thread_next_worklist_end)
						{
							// grab a new chunk
							do
							{
								thread_next_worklist_index = next_worklist_length;
								thread_next_worklist_end = thread_next_worklist_index + waspr;
							}while(__sync_val_compare_and_swap(&next_worklist_length, thread_next_worklist_index, thread_next_worklist_end) != thread_next_worklist_index);
							assert(next_worklist_length <= worklist_size);
						}
					}
				}	

				__sync_fetch_and_add(&next_vertices, thread_next_vertices, __ATOMIC_SEQ_CST);

				// fill unused indecis with -1U to prevent from being processed in the next iteration
				while(thread_next_worklist_index < thread_next_worklist_end)
					next_worklist[thread_next_worklist_index++] = -1U;

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
			{
				char temp[255];
				sprintf(temp, "Push, |F|: %5u, time:", next_vertices);
				PTIP(temp);
			}
			cc_iter++;

			// swapping
				{
					unsigned int* temp = worklist;
					worklist = next_worklist;
					next_worklist = temp;

					worklist_length = next_worklist_length;
					next_worklist_length = waspr * pe->threads_count;

					unsigned char* temp2 = df;
					df = next_df;
					next_df = temp2;
				}
		}while(next_vertices);
		if(flags & 1U)
			printf("Max-degree in push iterations: \t\t%'u\n", push_max_degree);
		if(exec_info)
			exec_info[8] = push_max_degree;

	// Saving events
		if(!(flags & 2U))
		{
			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			if(flags & 1U)
				print_hw_events(pe, 1);
			if(exec_info)
				copy_reset_hw_events(pe, &exec_info[1], 1);
		}

	// Counting number of components
		if(ccs_p)
		{
			unsigned int ccs = 1;
			#pragma omp parallel for reduction(+:ccs)
			for(unsigned int v = 0; v < g->vertices_count; v++)
				if(cc[v] == v + 1)
					ccs++;
			printf("|CCs|:            \t\t%'u\n",ccs);
			*ccs_p = ccs;
		}
		
	// Finalizing
		t0 += get_nano_time();
		printf("Total exec. time: \t\t %'.1f (ms)\n\n",t0/1e6);
		if(exec_info)
			exec_info[0] = t0;

	// Releasing memory
		free(partitions);
		partitions = NULL;
		free(ttimes);
		ttimes = NULL;
		numa_free(worklist, sizeof(unsigned int) * worklist_size);
		worklist = NULL;
		numa_free(next_worklist, sizeof(unsigned int) * worklist_size);
		next_worklist = NULL;
		numa_free(df, sizeof(unsigned char) * g->vertices_count);
		df = NULL;
		numa_free(next_df, sizeof(unsigned char) * g->vertices_count);
		next_df = NULL;

	return cc;
}

void cc_release(struct ll_400_graph* g, unsigned int* cc)
{
	assert(cc != NULL && g != NULL);
	numa_free(cc, sizeof(unsigned int) * g->vertices_count);
	return;
} 

/*
	An implementation of a Disjoint-Set Union CC 
	introduced by Siddhartha Jayanti and Robert Tarjan in the following paper

	@article{ DBLP:journals/corr/JayantiT16,
	author    = {Siddhartha V. Jayanti and Robert E. Tarjan},
	title     = {A Randomized Concurrent Algorithm for Disjoint Set Union},
	journal   = {CoRR},
	volume    = {abs/1612.01514},
	year      = {2016},
	url       = {http://arxiv.org/abs/1612.01514},
	eprinttype = {arXiv},
	eprint    = {1612.01514},
	}

	flags:
		bit 0: print stats
		bit 1: do not reset papi

	exec_info: if not NULL, will have
		[0]: exec time
		[1-7]: papi events
*/

unsigned int* cc_jt(struct par_env* pe, struct ll_400_graph* g, unsigned int flags, unsigned long* exec_info, unsigned int* ccs_p)
{
	// Initial checks
		assert(pe != NULL && g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;31mcc_jt\033[0;37m\n");

	// Reset papi
		if(!(flags & 2U))
			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				papi_reset(pe->papi_args[tid]);
			}
	
	// Allocate memory
		unsigned int* cc = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(cc != NULL);
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// Edge partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(g, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// (1) Initializing
		unsigned long mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			unsigned int partition = -1U;		
			
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					cc[v] = v;
				
			}

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		if(flags & 1U)
			PTIP("(1) Initializing");

	// (2) Traversing edges
		mt = - get_nano_time();
		#pragma omp parallel  
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();
			unsigned int partition = -1U;		
			
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
					{
						unsigned int neighbour = g->edges_list[e];
						if(neighbour >= v)
							break;

						unsigned int x = v;
						unsigned int y = neighbour;

						while(1)
						{
							while(x != cc[x])
								x = cc[x];

							while(y != cc[y])
								y = cc[y];

							if(x == y)
								break;

							if(x < y)
							{
								if(__sync_bool_compare_and_swap(&cc[y], y, x))
									break;
							}
							else
							{
								if(__sync_bool_compare_and_swap(&cc[x], x, y))
									break;
							}

						}
					}
			}

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		if(flags & 1U)
			PTIP("(2) Traversing edges:");

	// (3) Pointer jumping
		mt = - get_nano_time();
		#pragma omp parallel   
		{
			unsigned tid = omp_get_thread_num();
			ttimes[tid] = - get_nano_time();

			#pragma omp for 
			for(unsigned int v = 0; v < g->vertices_count; v++)
				while(cc[cc[v]] != cc[v])
					cc[v] = cc[cc[v]];

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		if(flags & 1U)
			PTIP("(3) Pointer jumping:");

	// Saving events
		if(!(flags & 2U))
		{
			#pragma omp parallel
			{
				assert(0 == thread_papi_read(pe));
			}
			if(flags & 1U)
				print_hw_events(pe, 1);
			if(exec_info)
				copy_reset_hw_events(pe, &exec_info[1], 1);
		}

	// Counting number of components
		if(ccs_p)
		{
			unsigned int ccs = 0;
			#pragma omp parallel for reduction(+:ccs)
			for(unsigned int v = 0; v < g->vertices_count; v++)
				if(cc[v] == v)
					ccs++;
			printf("|CCs|:            \t\t%'u\n",ccs);
			*ccs_p = ccs;
		}
		
	// Finalizing
		t0 += get_nano_time();
		printf("Total exec. time: \t\t %'.1f (ms)\n\n",t0/1e6);
		if(exec_info)
			exec_info[0] = t0;

	// Releasing memory
		free(partitions);
		partitions = NULL;
		free(ttimes);
		ttimes = NULL;
	
	return cc;
}

#endif
