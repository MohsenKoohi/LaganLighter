#ifndef __TRANS_C
#define __TRANS_C

#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <assert.h>
#include <time.h>

#include "omp.c"
#include "partitioning.c"


void sort_neighbor_lists(struct par_env* pe, struct ll_400_graph* g)
{
	assert(pe != NULL && g!= NULL);

	// Allocating mem
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;	
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(g, partitions, partitions_count);
		
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Sorting
	unsigned long mt = - get_nano_time();
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
				unsigned int degree = g->offsets_list[v+1] - g->offsets_list[v];
				if(degree < 2)
					continue;
				quick_sort_uint(&g->edges_list[g->offsets_list[v]], 0, degree - 1);
			}
		}
		ttimes[tid] += get_nano_time();
	}
	mt += get_nano_time();
	PTIP("Sorting");

	// Releasing mem
		free(partitions);
		partitions = NULL;
		free(ttimes);
		ttimes = NULL;
		dynamic_partitioning_release(dp);
		dp = NULL;

	return;	
}

/*
	Validates the transposition of `g` to `t`
	returns `1` as true, and `0` as false

	`flags`:
		bit 0: no self-edges
*/
int validate_transposition(struct par_env* pe, struct ll_400_graph* g, struct ll_400_graph* t, unsigned int flags)
{
	assert(pe != NULL && g != NULL && t != NULL);

	// Allocating mem
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;	
		unsigned int* g_partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(g_partitions != NULL);
		parallel_edge_partitioning(g, g_partitions, partitions_count);

		unsigned int* t_partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(t_partitions != NULL);
		parallel_edge_partitioning(t, t_partitions, partitions_count);
		
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);
		int ret = 1;

	// Validation
		// Initial checks
			assert(t->vertices_count == g->vertices_count);
			assert(t->offsets_list[0] == 0);
			assert(t->offsets_list[t->vertices_count] == t->edges_count);
			assert(t->edges_count <= g->edges_count);

		// Check if `g` is sorted
			unsigned long mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned long thread_se = 0;
				unsigned int partition = -1U;
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
					for(unsigned int v = g_partitions[partition]; v < g_partitions[partition + 1]; v++)		
						for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
							if(e < (g->offsets_list[v + 1] - 1))
								if(g->edges_list[e] >= g->edges_list[e + 1])
								{
									printf("v:%'u,  deg:%'lu,  eo:%'lu,  neighbour: %'u,  next-neighbour: %'u\n", v, g->offsets_list[v+1] - g->offsets_list[v], e, g->edges_list[e], g->edges_list[e+1]);
									assert(g->edges_list[e] < g->edges_list[e + 1] && "The input graph does not have sorted neighbour-lists");
									ret = 0;
								}
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("Validation 1, check input is sorted");
			if(!ret)
				goto validate_transposition_rel_mem;

		// If an edge is in `t` it should be in `g`
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
					for(unsigned int v = t_partitions[partition]; v < t_partitions[partition + 1]; v++)
					{
						assert(t->offsets_list[v+1] >= t->offsets_list[v]);
						for(unsigned long e = t->offsets_list[v]; e < t->offsets_list[v + 1]; e++)
						{
							unsigned int dest = v;
							unsigned int src = t->edges_list[e];

							if((flags & 1U) && src == dest)
							{
								printf("Validation 1 error: src == dest %'u->%'u\n", src, dest);
								assert(dest != src);
								ret = 0;
							}

							unsigned long found = uint_binary_search(g->edges_list, g->offsets_list[src], g->offsets_list[src + 1], dest);
							if(found == -1UL)
							{
								printf("Validation 1 error: cannot find %'u->%'u\n", src, dest);
								assert(found != -1UL);
								ret = 0;
							}
						}					
					}
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("Validation 2, output includes all edges of the input");
			if(!ret)
				goto validate_transposition_rel_mem;

		// If an edge is in `g` it should be in `t` for both endpoints 
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
					for(unsigned int v = g_partitions[partition]; v < g_partitions[partition + 1]; v++)
						for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
						{
							unsigned int src = v;
							unsigned int dest = g->edges_list[e];

							if((flags & 1U) && src == dest)
								continue;

							unsigned long found = uint_binary_search(t->edges_list, t->offsets_list[dest], t->offsets_list[dest + 1], src);
							if(found == -1UL)
							{
								printf("Validation 2 error: cannot find %'u->%'u\n", src, dest);
								assert(found != -1UL);
								ret = 0;
							}
						}	
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("Validation 3, input includes all edges of the output");

	// Releasing mem
	validate_transposition_rel_mem:

		free(t_partitions);
		t_partitions = NULL;

		free(g_partitions);
		g_partitions = NULL;
		
		free(ttimes);
		ttimes = NULL;
		
		dynamic_partitioning_release(dp);
		dp = NULL;

	return ret;	
}

/*
	The input graph should have sorted neighbour-lists as binary search is 
	used in order to check if a reverse edges exists.

	Before increasing symmetric degree of each vertex in Step 2, we search to see 
	if that edge exists in the neighbour-list of the destination.
	Total comlexity: O(|E|log(|E|/|V|)), assuming degree of each vertex is |E|/|V|.

	We assign a bit for each edge to specify if this edge is symmetric or not. 
	Using this bit array we do not repeat the search in Step 5.
	
	A faster version can rewrite all edges without searching for repeated edges 
	(i.e., we need dynamic memory for neighbour lists of each vertex), 
	but then after sorting we can remove repeated edges and rewrite the offsets list and filtered edges list.
	This requires more memory, but is faster. 
	Total complexity of this version will be: O((|E|/|V|)*log(|E|/|V|)).

	Another solution is to create the csc graph from csr (2|E| + |E|log(|E|/V|)), 
	and then creating symmetric graph from csc and csr. While the approximate complexity is the same as the first one,
	this solution is faster as in the first solution we search out-neighbour list of each out-neighbour, 
	but in the third solution, we sort in-neighbours of each vertex. Moreover, when we want to sort the output, 
	sorting the symmetric graph is more prone to load imbalance than the csc graph.

	flags: 
	bit 0 : validate results
	bit 1 : sort neighbour-list of the output  
	bit 2 : remove self-edges
*/
struct ll_400_graph* symmetrize_graph(struct par_env* pe, struct ll_400_graph* in_graph, unsigned int flags)
{
	// Initial checks
		unsigned long tt = - get_nano_time();
		assert(pe != NULL && in_graph != NULL);
		printf("\n\033[3;35msymmetrize_graph\033[0;37m using \033[3;35m%d\033[0;37m threads.\n", pe->threads_count);
		unsigned long free_mem = get_free_mem();
		if(free_mem < in_graph->edges_count * sizeof(unsigned int) + in_graph->vertices_count * sizeof(unsigned long))
		{
			printf("Not enough memory.\n");
			return NULL;
		}
		
	// Partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(in_graph, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Allocating memory
		struct ll_400_graph* out_graph =calloc(sizeof(struct ll_400_graph),1);
		assert(out_graph != NULL);
		out_graph->vertices_count = in_graph->vertices_count;
		out_graph->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + in_graph->vertices_count));
		assert(out_graph->offsets_list != NULL);

		unsigned long* last_offsets = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + in_graph->vertices_count));
		assert(last_offsets != NULL);

		unsigned char* edge_is_symmetric = numa_alloc_interleaved(sizeof(unsigned char) * ( 1 + in_graph->edges_count / 8));
		assert(edge_is_symmetric != NULL);

		unsigned long* partitions_total_edges = calloc(sizeof(unsigned long), partitions_count);
		assert(partitions_total_edges != NULL);

		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// (1) Checking if neighbour-lists are sorted and decrease degree for self-edges	
		unsigned long mt = - get_nano_time();
		unsigned long self_edges = 0;
		#pragma omp parallel  reduction(+:self_edges)
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
					long degree = in_graph->offsets_list[v+1] - in_graph->offsets_list[v];
					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
					{
						if(flags & 4U)  // remove self edges
							if(in_graph->edges_list[e] == v)
							{
								self_edges++;
								degree--;
							}

						if(e < (in_graph->offsets_list[v + 1] - 1))
							if(in_graph->edges_list[e] >= in_graph->edges_list[e + 1])
							{
								printf("v:%u deg:%lu eo:%lu neighbour:%u neighbour+1:%u\n", v, in_graph->offsets_list[v+1] - in_graph->offsets_list[v], e, in_graph->edges_list[e], in_graph->edges_list[e+1]);
								assert(in_graph->edges_list[e] < in_graph->edges_list[e + 1] && "The input graph does not have sorted neighbour-lists");
							}
					}
					assert(degree >= 0);
					out_graph->offsets_list[v] = degree;
				}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("1: Check");
		printf("%-20s \t\t\t %'10lu\n","Self edges:", self_edges);

	// (2) Identifying degree of vertices in the symmetric graph
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
					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
					{
						unsigned int dest = in_graph->edges_list[e];
						
						// self-edges are already in in_graph (if they should exist), so we do not add them again 
						if(dest == v)
							continue;

						// Check if the edge exists in the neighbour-list of the dest 
						if(uint_binary_search(in_graph->edges_list, in_graph->offsets_list[dest], in_graph->offsets_list[dest + 1], v) != -1UL)
						{
							// setting the bit in the edge_is_symmetric to prevent being searched again in Step 5
							unsigned char val = (((unsigned char)1)<<(e % 8));
							unsigned char val2 = __atomic_fetch_or(&edge_is_symmetric[e / 8], val, __ATOMIC_RELAXED);
							assert((val2 & val) == 0);
							continue;
						}

						// Increment the degree of dest
						__atomic_fetch_add(&out_graph->offsets_list[dest], 1UL, __ATOMIC_RELAXED);
					}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("2: Degree");
		
	// (3) Storing the total edges of each partition in partitions_total_edges
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
		PTIP("3: Sum");
	
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
			printf("%-20s \t\t\t %'10lu\n","Symmetric edges:", out_graph->edges_count);
		}
		out_graph->offsets_list[out_graph->vertices_count] = out_graph->edges_count;
		out_graph->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * out_graph->edges_count);
		assert(out_graph->edges_list != NULL);

	// (4) Updating the last_offsets and out_graph->offsets_list and copying in_graph edges of each vertex
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
				unsigned long current_offset = partitions_total_edges[partition];
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned long sym_degree = out_graph->offsets_list[v];
					out_graph->offsets_list[v] = current_offset;
					unsigned long last_offset = current_offset;
					current_offset += sym_degree;

					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v+1]; e++)
					{
						unsigned int neighbour = in_graph->edges_list[e];
						if( (flags & 4U) && neighbour == v)
							continue;
						out_graph->edges_list[last_offset++] = neighbour;
					}
					
					last_offsets[v] = last_offset;
					assert(last_offset <= current_offset);
				}

				if(partition + 1 < partitions_count)
					assert(current_offset == partitions_total_edges[partition + 1]);
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("4: last_offsets");	

	// (5) Writing edges
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
					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
					{
						unsigned int src = v;
						unsigned int dest = in_graph->edges_list[e];

						// do not duplicate self edges
						if(src == dest)
							continue;

						if(edge_is_symmetric[e / 8] & ( ((unsigned char)1) << (e % 8) ) )
						// if(uint_binary_search(in_graph->edges_list, in_graph->offsets_list[dest], in_graph->offsets_list[dest + 1], src) != -1UL) 
							continue;

						unsigned long prev_offset = __atomic_fetch_add(&last_offsets[dest], 1UL, __ATOMIC_RELAXED);
						assert(prev_offset < out_graph->offsets_list[dest+1]);
						out_graph->edges_list[prev_offset] = src;
					}
			}

			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("5: Writing edges");

	// Sorting
		if((flags & 2U))
		{	
			mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				unsigned int partition = -1U;	
				
				#pragma omp for nowait schedule(static, 16)
				for(unsigned int v = 0; v < out_graph->vertices_count; v++)
				{
					unsigned int degree = out_graph->offsets_list[v+1] - out_graph->offsets_list[v];
					if(degree < 2)
						continue;
					quick_sort_uint(&out_graph->edges_list[out_graph->offsets_list[v]], 0, degree - 1);
				}
				
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();

			PTIP("Sorting");
		}

	// Validation
		if((flags & 1U))
		{	
			assert(out_graph->vertices_count == in_graph->vertices_count);
			assert(out_graph->offsets_list[0] == 0);
			assert(out_graph->offsets_list[out_graph->vertices_count] == out_graph->edges_count);

			// If an edge is in out_graph it should be in in_graph 
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
						assert(out_graph->offsets_list[v+1] == last_offsets[v]);
						assert(out_graph->offsets_list[v+1] >= out_graph->offsets_list[v]);
						for(unsigned long e = out_graph->offsets_list[v]; e < out_graph->offsets_list[v + 1]; e++)
						{
							unsigned int dest = v;
							unsigned int src = out_graph->edges_list[e];

							if((flags & 4U) && src == dest)
							{
								printf("Validation error: src == dest %'u->%'u\n", src, dest);
								assert(dest != src);
							}

							// it can be an edge in the neighbour-list of src						
							unsigned long found =  uint_binary_search(in_graph->edges_list, in_graph->offsets_list[src], in_graph->offsets_list[src + 1], dest);
							if(found != -1UL)
								continue;

							// it can be an edge in the neighbour-list of dest
							found =  uint_binary_search(in_graph->edges_list, in_graph->offsets_list[dest], in_graph->offsets_list[dest + 1], src);
							if(found == -1UL)
							{
								printf("Validation error: cannot find %'u->%'u\n", src, dest);
								assert(found != -1UL);
							}
						}					
					}
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("Validation 1");

			// If an edge is in in_graph it should be in out_graph for both endpoints 
			assert((flags & 2U) && "Neighbour-list should be sorted for the second evaluation.");
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
						for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
						{
							unsigned int src = v;
							unsigned int dest = in_graph->edges_list[e];

							if((flags & 4U) && src == dest)
								continue;

							unsigned long found = uint_binary_search(out_graph->edges_list, out_graph->offsets_list[src], out_graph->offsets_list[src + 1], dest);
							assert(found != -1UL);

							found = uint_binary_search(out_graph->edges_list, out_graph->offsets_list[dest], out_graph->offsets_list[dest + 1], src);
							assert(found != -1UL);
						}	
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("Validation 2");
		}

	// Releasing memory
		free(partitions);
		partitions = NULL;

		dynamic_partitioning_release(dp);
		dp = NULL;

		numa_free(last_offsets, (1 + in_graph->vertices_count) * sizeof(unsigned long));
		last_offsets = NULL;
		
		free(partitions_total_edges);
		partitions_total_edges = NULL;

		numa_free(edge_is_symmetric, sizeof(unsigned char) * ( 1 + in_graph->edges_count / 8));
		edge_is_symmetric = NULL;

		free(ttimes);
		ttimes = NULL;

	// Finalizing
		tt += get_nano_time();
		printf("%-20s \t\t\t %'.3f (s)\n\n","Total time:", tt/1e9);
		print_ll_400_graph(out_graph);

	return out_graph;
}

/*
	atomic_transpose() has two passes over edges to identify degree of vertex and then to write neighbour-lists.
	Total complexity is 2|E| plus |E|log(|E|/|V|) if neighbour-lists should be sorted.

	flags: 
		bit 0 : validate results (requires bit 1 to be set)
		bit 1 : sort neighbour-list of the output  
		bit 2 : remove self-edges
		bit 3 : only create offsets_list of the out_graph and do not write edges
*/
struct ll_400_graph* atomic_transpose(struct par_env* pe, struct ll_400_graph* in_graph, unsigned int flags)
{
	// Initial checks
		unsigned long tt = - get_nano_time();
		assert(pe != NULL && in_graph != NULL);
		printf("\n\033[3;35matomic_transpose\033[0;37m using \033[3;35m%d\033[0;37m threads.\n", pe->threads_count);

	// Partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(in_graph, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Allocating memory
		struct ll_400_graph* out_graph =calloc(sizeof(struct ll_400_graph),1);
		assert(out_graph != NULL);
		out_graph->vertices_count = in_graph->vertices_count;
		out_graph->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + in_graph->vertices_count));
		assert(out_graph->offsets_list != NULL);

		unsigned long* partitions_total_edges = calloc(sizeof(unsigned long), partitions_count);
		assert(partitions_total_edges != NULL);

		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

	// (1) Identifying degree of vertices in the out_graph
		unsigned long self_edges = 0;
		unsigned long mt = - get_nano_time();
		#pragma omp parallel  reduction(+:self_edges)
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
					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
					{
						unsigned int dest = in_graph->edges_list[e];
						
						if(dest == v)
						{
							self_edges++;
							if(flags & 4U)  // remove self edges
								continue;
						}

						// Increment the degree of dest
						__atomic_fetch_add(&out_graph->offsets_list[dest], 1UL, __ATOMIC_RELAXED);
					}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("(1) Identifying degrees");
		printf("%-20s \t\t\t %'10lu\n","Self edges:", self_edges);
		
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
			printf("%-20s \t\t\t %'10lu\n","out_graph edges:", out_graph->edges_count);
		}
		out_graph->offsets_list[out_graph->vertices_count] = out_graph->edges_count;

	// (3) Updating the out_graph->offsets_list
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
				unsigned long current_offset = partitions_total_edges[partition];
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned long t_degree = out_graph->offsets_list[v];
					out_graph->offsets_list[v] = current_offset;
					current_offset += t_degree;
				}

				if(partition + 1 < partitions_count)
					assert(current_offset == partitions_total_edges[partition + 1]);
				else
					assert(current_offset == out_graph->edges_count);
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("(3) Update offsets_list");

	if(flags & 8U)
		goto atomic_transpose_release;

	out_graph->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * out_graph->edges_count);
	assert(out_graph->edges_list != NULL);
		
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
					for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
					{
						unsigned int src = v;
						unsigned int dest = in_graph->edges_list[e];

						if(src == dest)
							if(flags & 4U)  // remove self edges
								continue;

						unsigned long prev_offset = __atomic_fetch_add(&out_graph->offsets_list[dest], 1UL, __ATOMIC_RELAXED);
						assert(prev_offset < out_graph->offsets_list[dest+1]);
						out_graph->edges_list[prev_offset] = src;
					}
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("(4) Writing edges");

	// (5) Updating the out_graph->offsets_list
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
				unsigned long current_offset = partitions_total_edges[partition];
				for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
				{
					unsigned long next_vertex_offset = out_graph->offsets_list[v];
					out_graph->offsets_list[v] = current_offset;
					current_offset = next_vertex_offset;
				}

				if(partition + 1 < partitions_count)
					assert(current_offset == partitions_total_edges[partition + 1]);
				else
					assert(current_offset == out_graph->edges_count);
			}
			ttimes[tid] += get_nano_time();
		}
		mt += get_nano_time();
		dynamic_partitioning_reset(dp);
		PTIP("(5) Updating offsets_list");	

	// (6) Sorting
		if(flags & (2U | 1U))
			sort_neighbor_lists(pe, out_graph);
			
	// Validation
		if((flags & 1U))
		{	
			assert(flags & 2U);
			
			unsigned int tf = 0;
			if(flags & 4U)
				tf = 1U;
			int ret = validate_transposition(pe, in_graph, out_graph, tf);
			if(ret != 1)
			{
				printf("  Validation failed.\n");	
				assert(ret == 1);
			}
		}

	// Releasing memory
	atomic_transpose_release: 

		free(partitions);
		partitions = NULL;

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

/* 
	Add random weights to the graph

	Note that the neighbour lists are not sorted
	
	Flags:
		0 : validate
*/
struct ll_404_graph* add_4B_weight_to_ll_400_graph(struct par_env* pe, struct ll_400_graph* g, unsigned int max_weight, unsigned int flags)
{
	assert(pe != NULL && g != NULL && max_weight != 0);
	printf("\n\033[3;36madd_4B_weight_to_graph\033[0;37m, wieght_val: \033[3;36m%'u\033[0;37m .\n",  max_weight);

	unsigned long tt = - get_nano_time();

	// Memory allocation
		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);
	
	// Creating graph
		struct ll_404_graph* graph =calloc(sizeof(struct ll_404_graph),1);
		assert(graph != NULL);
		graph->vertices_count = g->vertices_count;
		graph->edges_count = g->edges_count;
		graph->offsets_list = numa_alloc_interleaved(sizeof(unsigned long)*(1 + g->vertices_count));
		assert(graph->offsets_list != NULL);
		graph->edges_list = numa_alloc_interleaved(2 * sizeof(unsigned int) * g->edges_count);
		assert(graph->edges_list != NULL);
	
	// Assigning weights
		// Assign weights for neighbours of each vertex with IDs smaller than the ID of that vertex 
			// Partitioning
			unsigned int thread_partitions = 64;
			unsigned int partitions_count = pe->threads_count * thread_partitions;
			unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
			assert(partitions != NULL);
			parallel_edge_partitioning(g, partitions, partitions_count);
			struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

			unsigned long mt = - get_nano_time();
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

					// Preparing the seed for randomization
						// We create random weights that remain the same in different executions
						// So for each partition we create a repeatable seed
						unsigned long s[4];
						rand_initialize_splitmix64(s, partition);

					// Iterating over vertices in this partition
					for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)		
					{
						unsigned long e = g->offsets_list[v];
						for(; e < g->offsets_list[v+1]; e++)
						{
							if(g->edges_list[e] > v)
								break;

							unsigned long rand_val = rand_xoshiro256(s);

							graph->edges_list[2 * e] = g->edges_list[e];
							graph->edges_list[2 * e + 1] = 1 + (rand_val % max_weight);
						}
						graph->offsets_list[v] = e;
					}
				}
				ttimes[tid] += get_nano_time();
			}
			dynamic_partitioning_release(dp);
			dp = NULL;
			free(partitions);
			partitions = NULL;
			mt += get_nano_time();
			PTIP("Step 1: Assigning weights");
		
		// Writing the weights for symmetric edges of each vertex with neighbour ID > vertex ID
			mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				
				#pragma omp for nowait schedule(static, 8)
				for(unsigned int v=0; v < graph->vertices_count; v++)
					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v+1]; e++)
					{
						if(g->edges_list[e] >= v)
							break;

						unsigned int neighbour = (unsigned int)g->edges_list[e];
						// assert(graph->edges_list[2 * e] == neighbour);
						unsigned long neighbour_offset = __atomic_fetch_add(&graph->offsets_list[neighbour], 1U, __ATOMIC_RELAXED);
						// assert(neighbour_offset < g->offsets_list[neighbour + 1]);

						graph->edges_list[2 * neighbour_offset] = v;
						graph->edges_list[2 * neighbour_offset + 1] = graph->edges_list[2 * e + 1];
					}
				
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			PTIP("Step 2: Symmetrizing weights");
		
		// Validation
			if(flags & 1U)
			{
				#pragma omp parallel for 
				for(unsigned int v=0; v < graph->vertices_count; v++)
					assert(graph->offsets_list[v] == g->offsets_list[v+1]);

				#pragma omp parallel for 
				for(unsigned int v=0; v < graph->vertices_count; v++)
					for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v+1]; e++)
					{
						if(g->edges_list[e] >= v)
							break;

						unsigned int neighbour = g->edges_list[e];
						assert(graph->edges_list[2 * e] == neighbour);
						
						unsigned int found = 0;
						for(unsigned long e2 = g->offsets_list[neighbour]; e2 < g->offsets_list[neighbour + 1]; e2++)
							if(graph->edges_list[2 * e2] == v)
							{
								assert(graph->edges_list[2 * e2 + 1] == graph->edges_list[2 * e + 1]);
								found = 1;
								break;
							}

						assert(found == 1);
					}

				printf("Validated.\n");
			}
	
	// Setting the offsets
		#pragma omp parallel for 
		for(unsigned int v=0; v <= graph->vertices_count; v++)
			graph->offsets_list[v] = g->offsets_list[v];

	// Free mem
		free(ttimes);
		ttimes = NULL;

	// Finialzing
		tt += get_nano_time();
		printf("%-20s \t\t\t %'.3f (s)\n","Total time:", tt/1e9);
		print_ll_400_graph((struct ll_400_graph*)graph);
		
	return graph;
}

struct ll_400_graph* copy_ll_400_graph(struct par_env* pe, struct ll_400_graph* in, struct ll_400_graph* out)
{
	if(out == NULL)
	{
		out =calloc(sizeof(struct ll_400_graph),1);
		assert(out != NULL);
		out->vertices_count = in->vertices_count;
		out->edges_count = in->edges_count;
		out->offsets_list = numa_alloc_interleaved(sizeof(unsigned long)*(1 + in->vertices_count));
		assert(out->offsets_list != NULL);
		out->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * in->edges_count);
		assert(out->edges_list != NULL);
	}
	else
	{
		assert(out->vertices_count == in->vertices_count && out->offsets_list != NULL);
		assert(out->edges_count == in->edges_count && out->edges_list != NULL);
	}

	#pragma omp parallel for 
	for(unsigned int v=0; v <= out->vertices_count; v++)
		out->offsets_list[v] = in->offsets_list[v];

	#pragma omp parallel for 
	for(unsigned long e=0; e < out->edges_count; e++)
		out->edges_list[e] = in->edges_list[e];

	return out;
}

struct ll_404_graph* copy_ll_404_graph(struct par_env* pe, struct ll_404_graph* in, struct ll_404_graph* out)
{
	if(out == NULL)
	{
		out =calloc(sizeof(struct ll_404_graph),1);
		assert(out != NULL);
		out->vertices_count = in->vertices_count;
		out->edges_count = in->edges_count;
		out->offsets_list = numa_alloc_interleaved(sizeof(unsigned long)*(1 + in->vertices_count));
		assert(out->offsets_list != NULL);
		out->edges_list = numa_alloc_interleaved(2 * sizeof(unsigned int) * in->edges_count);
		assert(out->edges_list != NULL);
	}
	else
	{
		assert(out->vertices_count == in->vertices_count && out->offsets_list != NULL);
		assert(out->edges_count == in->edges_count && out->edges_list != NULL);
	}

	#pragma omp parallel for 
	for(unsigned int v=0; v <= out->vertices_count; v++)
		out->offsets_list[v] = in->offsets_list[v];

	#pragma omp parallel for 
	for(unsigned long e=0; e < 2 * out->edges_count; e++)
		out->edges_list[e] = in->edges_list[e];

	return out;
}

/*
	This function removes weights of a ll_404 graph
*/
struct ll_400_graph* copy_ll_404_to_400_graph(struct par_env* pe, struct ll_404_graph* in, struct ll_400_graph* out)
{
	if(out == NULL)
	{
		out = calloc(sizeof(struct ll_400_graph),1);
		assert(out != NULL);
		out->vertices_count = in->vertices_count;
		out->edges_count = in->edges_count;
		out->offsets_list = numa_alloc_interleaved(sizeof(unsigned long)*(1 + in->vertices_count));
		assert(out->offsets_list != NULL);
		out->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * in->edges_count);
		assert(out->edges_list != NULL);
	}
	else
	{
		assert(out->vertices_count == in->vertices_count && out->offsets_list != NULL);
		assert(out->edges_count == in->edges_count && out->edges_list != NULL);
	}

	#pragma omp parallel for 
	for(unsigned int v=0; v <= out->vertices_count; v++)
		out->offsets_list[v] = in->offsets_list[v];

	#pragma omp parallel for 
	for(unsigned long e = 0; e < out->edges_count; e++)
		out->edges_list[e] = in->edges_list[2 * e];

	return out;
}

#endif