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
#include "relabel.c"
#include "energy.c"

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
	PoTra

	@misc{PoTra,
		title={On Optimizing Locality of Graph Transposition on Modern Architectures}, 
		author={Mohsen {Koohi Esfahani} and Hans Vandierendonck},
		year={2025},
		eprint={2501.06872},
		archivePrefix={arXiv},
		primaryClass={cs.DC},
		url={https://arxiv.org/abs/2501.06872},
		doi={10.48550/arXiv.2501.06872} 
	} 

	Arguments:
		flags: 
			bit 0: validate results (requires bit 1 to be set)
			bit 1: sort neighbour-list of the output  
			bit 2: remove self-edges
			bit 3: only create offsets_list of the out_graph and do not write edges
			bit 4: force HLH
			bit 5: force Atomic

		exec_info: an array of 40
			[0] : total exec. time without sorting and validation
			[1-9]: PAPI events
			[10-17]: Steps exec time (in nanoseconds)
				10: Step 1: Counting degrees 
				11: Step 2: Identifying offsets_file
				12: Step 3: Writing edges
				13:	Sorting
				14: Validation
				15: Step 1.1: Sampling time
				16: --- 
				17: Step 1.2: Creating hash table
			
			[18-19]: Energy measurement
				18: DRAM energy in Joules
				19: pacakges energy in Joules

			[20-40]: Other info
				20: Sampled edges
				21: Max degree of sampled grpah
				22: Sum of degrees of k-top vertices in the sampled graph
				23: @INSERT: Collissions: total number of collision
				24: @INSERT: Longest collision
				25: @INSERT: Collided insertions: inserts with at least 1 collision
				26: MSP_warmup_partitions
				27: MSP_test_partitions
				28: Average diff
				29: @EDGES: Collissions 
				30: @EDGES: Longest collision 
				31: @EDGES: Collided vertices
				32: reverse of load factor
				33: k
				34: Load imbalance in 3.1
				35: Edges of k-top vertices
				36: cache_bytes_per_HDV
				37: MSP speedup
				38: MSP result > 0 HLH(Hash-based LDV/HDV), <0 => Atomic
				39: ---

	Questions/Problems/Future Improvements:
		- For some graphs such as MS50, the main improvement of HLH is on Step 3 and in Step 1 atomic is better. 
		So our MSP (Method Selection Procedure) cannot detect it ...
		- For MS50 AD/kV is very small (4-6) except for its CSC version which is around 240
		- Load imbalance in 3.1 
		- The large speedup on Clueweb12,CSR is not very clear.
*/

struct ll_400_graph* potra(struct par_env* pe, struct ll_400_graph* in_graph, unsigned int flags, unsigned long* exec_info)
{
	#ifdef _ENERGY_MEASUREMENT
		struct energy_measurement* em = energy_measurement_init();
		energy_measurement_start(em);
	#endif

	// Initialization
		unsigned long tt = - get_nano_time();
		assert(pe != NULL && in_graph != NULL);
		
		printf("\n\033[3;35mpotra\033[0;37m using \033[3;35m%d\033[0;37m threads, flags: %x.\n", pe->threads_count, flags);
		
	
		// Reset papi
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			papi_reset(pe->papi_args[tid]);
		}

	// Constants
		const unsigned long edges_to_be_sampled = 0.05 * in_graph -> vertices_count;
		const double hash_table_load_factor = 0.5;
		const unsigned int cache_bytes_per_HDV = 4;
		const unsigned long max_k =  
			(pe->L3_caches_total_size + pe->L2_caches_total_size) / 
			(8 / hash_table_load_factor + cache_bytes_per_HDV *  pe->threads_count);
		const unsigned long k = min(max_k, in_graph->vertices_count);
		const unsigned long hash_table_records = k / hash_table_load_factor;
		const unsigned long hash_table_size = hash_table_records * sizeof(unsigned int) * 2;
		
		printf("\n");
		printf("Constants: \n");
		printf("  Edges to be sampled:       %'lu\n", edges_to_be_sampled); 
		printf("  Sampling rate:             %.4f |V|, %.4f |E|\n", 
			1.0 * edges_to_be_sampled / in_graph->vertices_count, 
			1.0 * edges_to_be_sampled / in_graph->edges_count
		);
		printf("  #Bytes per HDV:            %'u\n", cache_bytes_per_HDV);
		printf("  Max k (for top k-vertex):  %'u\n", max_k);
		printf("  K (for top k-vertex):      %'u\n", k);
		printf("  Hash table load factor:    %'.3f\n", hash_table_load_factor);
		printf("  Hash table records:        %'lu\n", hash_table_records);
		printf("  Hash table size:           %'.3f MB \n", hash_table_size / (1024. * 1024));

		const unsigned int MSP_warmup_partitions = 1;
		const unsigned int MSP_test_partitions = 1;
		const double min_test_speedup_for_HLH = 0.75;
		const double min_avg_diff_for_HLH = 0.03;
		printf("  MSP warmup partitions:     %u\n", MSP_warmup_partitions);
		printf("  MSP test partitions:       %u\n", MSP_test_partitions);
		printf("  Min. test speedup for HLH: %.3f\n", min_test_speedup_for_HLH);
		printf("  Min. avg. diff for HLH:    %.3f\n", min_avg_diff_for_HLH);

		printf("\n");

		if(exec_info)
		{
			exec_info[32] = 1/ hash_table_load_factor;
			exec_info[33] = k;
			exec_info[36] = cache_bytes_per_HDV;

			exec_info[26] = MSP_warmup_partitions;
			exec_info[27] = MSP_test_partitions;
		}
		
	// Partitioning
		unsigned int thread_partitions = 64 * 4;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(partitions != NULL);
		parallel_edge_partitioning(in_graph, partitions, partitions_count);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);

	// Set processing method based on the input flags and/or graph vertices
		int pm = 0;
		{
			if(flags & (1U << 4))
				pm = 1;
			else if(flags & (1U << 5))
				pm = -1;
			else if(in_graph->vertices_count * cache_bytes_per_HDV < pe->L3_caches_total_size + pe->L2_caches_total_size)
				pm = -1;

			if(pm != 0)
			{
				printf("  Selected processing method(pm): %d\n", pm);
				exec_info && (exec_info[38] = pm);
			}
		}

	// Allocating memory
		struct ll_400_graph* out_graph = NULL;
		unsigned long* partitions_edges = NULL;
		unsigned long* ttimes = NULL;
		int* partition2thread = NULL;				// >= 0 if the partition processed in HLH method, and < 0, in case of Atomic

		unsigned int* hash_table = NULL;			
		unsigned short** threads_high_counters = NULL;
		unsigned short ** threads_low_counters = NULL;
		unsigned int* ldv_counters = NULL;
		unsigned long ** threads_HDV_offsets = NULL;

		{
			unsigned long mt = - get_nano_time();

			out_graph = calloc(sizeof(struct ll_400_graph),1);
			assert(out_graph != NULL);
			out_graph->vertices_count = in_graph->vertices_count;
			out_graph->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + in_graph->vertices_count));
			assert(out_graph->offsets_list != NULL);

			partitions_edges = calloc(sizeof(unsigned long), partitions_count);
			assert(partitions_edges != NULL);

			ttimes = calloc(sizeof(unsigned long), pe->threads_count);
			assert(ttimes != NULL);

			ldv_counters = numa_alloc_interleaved(sizeof(unsigned int) * (in_graph->vertices_count + 1));
			assert(ldv_counters != NULL);

			partition2thread = calloc(sizeof(int), partitions_count);
			assert(partition2thread != NULL);

			if(pm != -1)
			{
				hash_table = numa_alloc_interleaved(hash_table_size);
				assert(hash_table != NULL);

				threads_high_counters = calloc(sizeof(unsigned short*), pe->threads_count);
				assert(threads_high_counters != NULL);
				threads_low_counters = calloc(sizeof(unsigned short*), pe->threads_count);
				assert(threads_low_counters != NULL);
				threads_HDV_offsets = calloc(sizeof(unsigned long*), pe->threads_count);
				assert(threads_HDV_offsets != NULL);
		
				#pragma omp parallel
				{
					unsigned int tid = omp_get_thread_num();
					unsigned int node_id = pe->thread2node[tid];
					
					threads_low_counters[tid] = numa_alloc_onnode(sizeof(unsigned short) * k, node_id);
					assert(threads_low_counters[tid] != NULL);
					threads_high_counters[tid] = numa_alloc_onnode(sizeof(unsigned short) * k, node_id);
					assert(threads_high_counters[tid] != NULL);
					threads_HDV_offsets[tid]= numa_alloc_onnode(sizeof(unsigned long) * k, node_id);
					assert(threads_HDV_offsets[tid] != NULL);


					for(unsigned int ki = 0; ki < k ; ki += 4096/sizeof(unsigned long))
					{
						threads_low_counters[tid][ki] = 0;
						threads_high_counters[tid][ki] = 0;
						threads_HDV_offsets[tid][ki] = 0;
					}
				}
			}

			#pragma omp parallel for
			for(unsigned int v = 0; v <= in_graph->vertices_count; v += 4096/sizeof(unsigned long))
			{
				ldv_counters[v] = 0;
				out_graph->offsets_list[v] = 0;
			}

			mt += get_nano_time();
			PT("  Allocating memory");
		}
		
	// (Step 1) Identifying degree of vertices in the out_graph
		unsigned long s0_time = - get_nano_time();
		// (Step 1.1) Selecting HDV
		unsigned int* RA = NULL;
		if(pm != -1)
		{
			// Sampling
				unsigned long t0 = - get_nano_time();
				unsigned long edges_per_thread = edges_to_be_sampled / pe->threads_count;
				unsigned long sampled_edges = 0;
				unsigned long mt = t0;
				unsigned long avg_diff = 0;
				unsigned int diff_count = 0;
				
				#pragma omp parallel reduction(+: sampled_edges) reduction(+: avg_diff) reduction(+: diff_count)
				{
					unsigned int tid = omp_get_thread_num();
					ttimes[tid] = - get_nano_time();

					unsigned long start_edge_index = (in_graph->edges_count / pe->threads_count) * tid ;
					unsigned long end_edge_index = (in_graph->edges_count / pe->threads_count) * (tid + 1);
					if(tid == pe->threads_count - 1)
						end_edge_index = in_graph->edges_count;
					unsigned long tec = end_edge_index - start_edge_index;

					unsigned long s[4];
					rand_initialize_splitmix64(s, pe->threads_count * get_nano_time() + tid);
			
					for (unsigned long ec = 0; ec < edges_per_thread; ec++)
					{	
						unsigned long ei = start_edge_index + rand_xoshiro256(s) % tec;
						unsigned int neighbor = in_graph->edges_list [ei];
		
						__atomic_fetch_add(out_graph->offsets_list + neighbor, 1UL, __ATOMIC_RELAXED);

						if(ei > 8)
						{
							unsigned int* cl = (unsigned int*)((unsigned long)(in_graph->edges_list + ei) & ~63UL);
							unsigned long diff = 0;
							for(int i = 0; i < 7; i++)
								diff += abs(cl[i] - cl[i+1]);
							diff /= 7;

							avg_diff += diff;
							diff_count++;
						}
					}
					
					sampled_edges = edges_per_thread;
					ttimes[tid] += get_nano_time();
				}
				out_graph->edges_count = sampled_edges;

				mt += get_nano_time();
				{
					exec_info && (exec_info[20] = sampled_edges);
					char temp[256];
					sprintf(temp, "    # Sampled Edges: %'lu.", sampled_edges);
					PTIP(temp);

					unsigned long ad = 1.0 * avg_diff / diff_count; // Average diff
					double adpv = 1.0 * ad / in_graph->vertices_count;  // Average diff per vertex
					printf("    Average diff: %'lu, /|V|: %.3f\n", ad, adpv);
					exec_info && (exec_info[28] = ad);
					
					if(pm == 0)
						if(adpv >= min_avg_diff_for_HLH )
						{
							pm = 1;
							printf("  Selected processing method(pm): %d\n", pm);
							exec_info && (exec_info[38] = pm);
						}
				}
			
			// Prefix sum
				mt = -get_nano_time();
				#pragma omp parallel for 
				for(unsigned int p = 0; p < partitions_count; p++)
				{
					unsigned int start_vertex = (out_graph->vertices_count/partitions_count) * p;
					unsigned int end_vertex = (out_graph->vertices_count/partitions_count) * (p + 1);
					if(p + 1 == partitions_count)
						end_vertex = out_graph->vertices_count;

					unsigned long sum = 0;
					for(unsigned int v = start_vertex; v < end_vertex; v++)
						sum += out_graph->offsets_list[v];

					partitions_edges[p] = sum;
				}

				{
					unsigned long ec = 0;
					for(unsigned int p = 0; p < partitions_count; p++)
					{
						unsigned long temp = partitions_edges[p];
						partitions_edges[p] = ec;
						ec += temp;
					}
					assert(ec == out_graph->edges_count);
				}

				#pragma omp parallel for 
				for(unsigned int p = 0; p < partitions_count; p++)
				{
					unsigned int start_vertex = (out_graph->vertices_count/partitions_count) * p;
					unsigned int end_vertex = (out_graph->vertices_count/partitions_count) * (p + 1);
					if(p + 1 == partitions_count)
						end_vertex = out_graph->vertices_count;

					unsigned long offset = partitions_edges[p];
					for(unsigned int v = start_vertex; v < end_vertex; v++)
					{
						unsigned long temp = out_graph->offsets_list[v];
						out_graph->offsets_list[v] = offset;
						offset += temp;
					}

					if(p + 1 == partitions_count)
					{
						assert(offset == out_graph->edges_count);
						out_graph->offsets_list[out_graph->vertices_count] = offset;
					}
					else
						assert(offset == partitions_edges[p + 1]);
				}
				mt += get_nano_time();
				PT("    Updating offsets_list");

			// Degree ordering with SAPCo
				mt = -get_nano_time();
				RA = sapco_sort_degree_ordering(pe, out_graph, NULL, 0);
				mt += get_nano_time();
				unsigned long sg_max_degree;
				{
					sg_max_degree = out_graph->offsets_list[RA[0] + 1] - out_graph->offsets_list[RA[0]];
					exec_info && (exec_info[21] = sg_max_degree);
					char temp[256];
					sprintf(temp, "    SAPCo sort, Max. degree: %'lu", sg_max_degree);
					PT(temp);
				}

			// Timing
				t0 += get_nano_time();
				mt = t0;
				PT("  (1.1) Sampling");
				exec_info && (exec_info[15] = mt);
		}

		// (Step 1.2) Filling the hash table
		if(pm != -1)
		{
			unsigned long mt = - get_nano_time();
			// Init hash records
			#pragma omp parallel for
			for(unsigned long r = 0; r < hash_table_records; r ++)
				hash_table[2 * r] = -1U;

			unsigned long cols = 0;
			unsigned long col_ins = 0;
			unsigned long longest_col = 0;
			unsigned long k_edges = 0;
			#pragma omp parallel for reduction(+: cols) reduction(max: longest_col) reduction(+: col_ins) reduction(+: k_edges)
			for(unsigned int ki = 0; ki < k; ki++)
			{
				unsigned int vertex_id = RA[ki];
				unsigned int s_degree = (out_graph->offsets_list[vertex_id + 1] - out_graph->offsets_list[vertex_id]);
				k_edges += s_degree;
				if(s_degree == 0)
					continue;

				// unsigned long s[4] = { vertex_id + 1, vertex_id + 1, vertex_id , vertex_id + 1};
				// rand_initialize_splitmix64(s,vertex_id + 1);
				// rand_xoshiro256(s);

				unsigned int s[2] = {vertex_id + 1, vertex_id + 2};
				// rand_xoroshiro64star(s);
				
				unsigned int c = 0;
				
				while(1)
				{
					unsigned int ht_index = rand_xoroshiro64star(s) % hash_table_records;
					if(__sync_bool_compare_and_swap(hash_table + 2 * ht_index, -1U, vertex_id))
					{
						hash_table[2 * ht_index + 1] = ki;
						break;
					}

					c++;
				}

				cols += c;
				if(c > longest_col)
					longest_col = c;
				if(c > 0)
					col_ins++;
			}

			if(exec_info)
			{
				exec_info[23] = cols;
				exec_info[24] = longest_col;
				exec_info[25] = col_ins;
			}

			printf("    k-top edges : %'lu (%.1f %%)\n", k_edges , 100.0 * k_edges / edges_to_be_sampled);
			exec_info && (exec_info[22] = k_edges);

			printf("    Collisions: %'lu, longest: %'lu, Collided inserts: %'lu (%.2f%%), Avg. Collision per k-top: %.2f \n", 
				cols, 
				longest_col, 
				col_ins, 
				100.0 * col_ins / k,
				1.0 * cols / k
			);

			mt += get_nano_time();
			exec_info && (exec_info[17] = mt);
			PT("  (1.2) Creating hash table");
		}

		// (Step 1.3) Counting
		unsigned long self_edges = 0;
		{
			unsigned long s1_3_time = - get_nano_time();

			unsigned int* threads_last_partition = calloc(sizeof(unsigned int), pe->threads_count);
			assert(threads_last_partition != NULL);
			for(int t = 0; t < pe->threads_count; t++)
				threads_last_partition[t] = -1U;
			
			unsigned long cols = 0;         // Total number of collisions
			unsigned long col_edges = 0;    // Edges with collisions
			unsigned long longest_col = 0;  // Longest collision

			unsigned long atomic_times = 0;
			unsigned long hlh_times = 0;
			unsigned long atomic_edges = 0;
			unsigned long hlh_edges = 0;
			
			/*
				We have 4 states:

				state = 0, warmup state: we process a/some partitions to make cache filled before deciding each of algorithms, we process in Atomic
				state = 1, HLH state: processing partition to measure exec time of HLH
				state = 2, Atomic state: processing partition to measure exec time of Atomic.
				At the end of state = 2, the Method Selection Procedure (MSP) is done to decide whether to continue with Atomic or HLH
				state = 3, processing the remained partitions based on the `pm`
			*/

			int state = 0;
			if(pm != 0)
				state = 3;

			for(; state < 4; state++)
			{
				unsigned long mt = - get_nano_time();
				unsigned int started_threads = 0;
				unsigned int finish_signal = 0;
				unsigned int state_partitions = 0;

				#pragma omp parallel  reduction(+:self_edges) reduction(+: cols) reduction(max: longest_col) reduction(+: col_edges)reduction(+:hlh_times) reduction(+:atomic_times) reduction(+:atomic_edges) reduction(+:hlh_edges) reduction(+:state_partitions)
				{
					unsigned int tid = omp_get_thread_num();
					ttimes[tid] = - get_nano_time();

					__atomic_fetch_add(&started_threads, 1U, __ATOMIC_RELAXED);

					unsigned int partition = -1U;
					
					unsigned int pp = 0; // processed partitions in this state

					while(1)
					{
						partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
						if(partition == -1U)
							break;

						if(state == 0 || state == 2 || (state == 3 && pm < 0))
						{
							// Processing in Atomic method
							partition2thread[partition] = -1;

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

									__atomic_fetch_add(&ldv_counters[dest], 1U, __ATOMIC_RELAXED);
								}
						}
						else
						{
							// Processing in HLH method
							partition2thread[partition] = tid;

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

									unsigned int s[2] = {dest + 1, dest + 2};
									
									unsigned int c = 0;				
									while(1)
									{
										unsigned int ht_index = rand_xoroshiro64star(s) % hash_table_records;
										unsigned int key = hash_table[2 * ht_index];
										
										// Not HDV, so, LDV
										if(key == -1U)
										{
											__atomic_fetch_add(&ldv_counters[dest], 1U, __ATOMIC_RELAXED);
											break;
										}

										// HDV
										if(key == dest)
										{
											unsigned int hdv_index = hash_table[2 * ht_index + 1];

											unsigned short new_val = ++threads_low_counters[tid][hdv_index];
											if(new_val == 0)
												threads_high_counters[tid][hdv_index]++;

											break;
										}

										c++;
									}

									cols += c;
									if(c > longest_col)
										longest_col = c;
									if(c > 0)
										col_edges++;
								}
						}


						if(state == 1)
							hlh_edges += in_graph->offsets_list[partitions[partition + 1]] - in_graph->offsets_list[partitions[partition]];
						if(state == 2)
							atomic_edges += in_graph->offsets_list[partitions[partition + 1]] - in_graph->offsets_list[partitions[partition]];

						pp++;
						state_partitions ++;

						if(state == 0 && pp >= MSP_warmup_partitions)
						{
							if(started_threads == pe->threads_count)
							{
								if(finish_signal == 0)
									finish_signal = 1;

								break;
							}
						}

						if((state == 1 || state == 2) && pp >= MSP_test_partitions)
						{
							if(started_threads == pe->threads_count)
							{
								if(finish_signal == 0)
									finish_signal = 1;

								break;
							}
						}
					}

					threads_last_partition[tid] = partition;

					ttimes[tid] += get_nano_time();

					if(state == 1)
						hlh_times += ttimes[tid];
					if(state == 2)
						atomic_times += ttimes[tid];
				}

				mt += get_nano_time();
				char temp[64];
				sprintf(temp, "    1.3, state = %u, partitions: %'u", state, state_partitions);
				PTIP(temp);

				// Selecting the processing method (MSP)
				if(state == 2)
				{
					pm = -1;
					double s = (1.0 * atomic_times / hlh_times) * (1.0 * hlh_edges / atomic_edges);
					if(s >= min_test_speedup_for_HLH)
						pm = 1;

					printf("  HLH_edges: %'lu, Atomic_edges: %'lu\n", hlh_edges, atomic_edges);
					printf("  MSP speedup (a/lh): %.2f, Selected processing method(pm): %d\n", s, pm);
					if(exec_info)
					{
						exec_info[37] = 1e9 * s;
						exec_info[38] = pm;
					}
				}

			}

			s1_3_time += get_nano_time();

			dynamic_partitioning_reset(dp);

			printf("    Collisions: %'lu, longest: %'lu, Collided edges: %'lu (%.2f%%), Avg. Collision per edge: %.2f \n", 
				cols, 
				longest_col, 
				col_edges, 
				100.0 * col_edges / in_graph->edges_count,
				1.0 * cols / in_graph->edges_count
			);

			if(exec_info)
			{
				exec_info[29] = cols;
				exec_info[30] = longest_col;
				exec_info[31] = col_edges;
			}
			
			printf("%-20s \t\t\t %'10lu\n","  Self edges:", self_edges);
			unsigned long mt = s1_3_time;
			PT("  (1.3) Identifying degrees");
		}
		s0_time += get_nano_time();
		{
			unsigned long mt = s0_time;
			PT("(1) Collecting degrees");
		}
		exec_info && (exec_info[10] = s0_time);

	// (Step 2) Identifying the insertion points 
		// (2.1) Calculating sum of edges of each partition in partitions_edges	
		unsigned long hdv_edges = 0;
		{
			unsigned long mt = - get_nano_time();
			exec_info && (exec_info[11] = mt);

			#pragma omp parallel  reduction(+:hdv_edges)
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();

				unsigned int partition = -1U;		
				while(1)
				{
					partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
					if(partition == -1U)
						break; 
				
					unsigned long sum = 0;

					if(hash_table == NULL)
					{
						for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
							sum += ldv_counters[v];
					}
					else
					{
						for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
						{
							unsigned int degree = 0;
							unsigned int s[2] = {v + 1, v + 2};							
							while(1)
							{
								unsigned int ht_index = rand_xoroshiro64star(s) % hash_table_records;
								unsigned int key = hash_table[2 * ht_index];
								
								// Not HDV, so, LDV
								if(key == -1U)
								{
									degree = ldv_counters[v];
									break;
								}

								// HDV
								if(key == v)
								{
									unsigned int hdv_index = hash_table[2 * ht_index + 1];
									
									degree = ldv_counters[v];
									for(unsigned int t = 0; t < pe->threads_count; t++)
										degree += (1U<<16) * threads_high_counters[t][hdv_index] + threads_low_counters[t][hdv_index];

									hdv_edges += degree;
									// ldv_counters[v] = degree;
													
									break;
								}
							}

							sum += degree;
						}
					}
					partitions_edges[partition] = sum;
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			PTIP("  (2.1) Calculating sum");
			dynamic_partitioning_reset(dp);

			exec_info && (exec_info[35] = hdv_edges);
		}

		// (2.2) Partial sum of partitions_edges
		{
			unsigned long sum = 0;
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned long temp = partitions_edges[p];
				partitions_edges[p] = sum;
				sum += temp;
			}
			out_graph->edges_count = sum;
			printf("%-20s \t\t\t %'10lu\n","    out_graph edges:", out_graph->edges_count);
			printf("%-20s \t\t\t %'10lu (%.2f%%)\n","    HDV edges:", hdv_edges, 100.0 * hdv_edges/out_graph->edges_count);

			out_graph->offsets_list[out_graph->vertices_count] = out_graph->edges_count;
		}
		
		// (2.3) Updating the out_graph->offsets_list
		{
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
					unsigned long current_offset = partitions_edges[partition];

					if(hash_table == NULL)
					{
						for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
						{
							out_graph->offsets_list[v] = current_offset;
							unsigned int degree = ldv_counters[v];
							current_offset += degree;
						}
					}
					else
					{
						for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
						{
							{
								unsigned int s[2] = {v + 1, v + 2};							
								while(1)
								{
									unsigned int ht_index = rand_xoroshiro64star(s) % hash_table_records;
									unsigned int key = hash_table[2 * ht_index];
									
									// Not HDV, so, LDV
									if(key == -1U)
									{
										out_graph->offsets_list[v] = current_offset;
										unsigned int degree = ldv_counters[v];
										current_offset += degree;
										
										break;
									}

									// HDV
									if(key == v)
									{
										unsigned int hdv_index = hash_table[2 * ht_index + 1];									
										
										for(unsigned int t = 0; t < pe->threads_count; t++)
										{
											threads_HDV_offsets[t][hdv_index] = current_offset;
											unsigned int degree = (1U<<16) * threads_high_counters[t][hdv_index] + threads_low_counters[t][hdv_index];
											current_offset += degree;
										}

										// For edges from partitions processed in Atomic method 
										out_graph->offsets_list[v] = current_offset;
										current_offset += ldv_counters[v];
													
										break;
									}
								}
							}
							
						}
					}

					if(partition + 1 < partitions_count)
						assert(current_offset == partitions_edges[partition + 1]);
					else
						assert(current_offset == out_graph->edges_count);
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("  (2.3) Update offsets_list");
			exec_info && (exec_info[11] += get_nano_time());
			mt = exec_info[11];
			PT("(2) Creating offsets_list");

			if(flags & 8U)
				goto potra_release;

			out_graph->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * out_graph->edges_count);
			assert(out_graph->edges_list != NULL);
		}
			
	// (Step 3) Writing edges 

		// (3.1) Writing edges
		{
			// Touching edges_list
			#pragma omp parallel for
			for(unsigned long e = 0; e < in_graph->edges_count ; e += 4096 / sizeof(unsigned int))
				out_graph->edges_list[e] = 0;

			/*
				We have two rounds:
					- Processing HLH partitions with partition2thread >= 0
					- Processing Atomic partitions with partition2thread < 0
			*/
			
			unsigned long mt = - get_nano_time();
			exec_info && (exec_info[12] = mt);
			#pragma omp parallel  
			{
				unsigned int tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				
				for(int round = 0; round < 2; round++)
				{
					for(unsigned int pi = 0; pi < partitions_count; pi++)
					{
						unsigned int partition = ( thread_partitions * tid + pi ) % partitions_count;
						if(round == 0)
						{
							if(partition2thread[partition] != tid)
								continue;

							for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
								for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
								{
									unsigned int src = v;
									unsigned int dest = in_graph->edges_list[e];

									if(src == dest)
										if(flags & 4U)  // remove self edges
											continue;

									unsigned long prev_offset = -1UL;
									unsigned int s[2] = {dest + 1, dest + 2};							
									while(1)
									{
										unsigned int ht_index = rand_xoroshiro64star(s) % hash_table_records;
										unsigned int key = hash_table[2 * ht_index];
										
										// Not HDV, so, LDV
										if(key == -1U)
										{
											prev_offset = __atomic_fetch_add(&out_graph->offsets_list[dest], 1UL, __ATOMIC_RELAXED);
											break;
										}

										// HDV
										if(key == dest)
										{
											unsigned int hdv_index = hash_table[2 * ht_index + 1];

											prev_offset = threads_HDV_offsets[tid][hdv_index]++;
															
											break;
										}
									}

									out_graph->edges_list[prev_offset] = src;
								}
						}

						if(round == 1)
						{
							if(!__sync_bool_compare_and_swap(partition2thread + partition, -1, tid))
								continue;

							for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
								for(unsigned long e = in_graph->offsets_list[v]; e < in_graph->offsets_list[v + 1]; e++)
								{
									unsigned int src = v;
									unsigned int dest = in_graph->edges_list[e];

									if(src == dest)
										if(flags & 4U)  // remove self edges
											continue;

									unsigned long prev_offset = __atomic_fetch_add(&out_graph->offsets_list[dest], 1UL, __ATOMIC_RELAXED);
									out_graph->edges_list[prev_offset] = src;
								}
						}
					}
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			PTIP("  (3.1) Writing edges");
			if(exec_info)
				exec_info[34] = get_idle_percentage(mt, ttimes, pe->threads_count);	
		}

		// (3.2) Updating the out_graph->offsets_list
		{
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
					unsigned long current_offset = partitions_edges[partition];
					for(unsigned int v = partitions[partition]; v < partitions[partition + 1]; v++)
					{
						unsigned long next_vertex_offset = out_graph->offsets_list[v];
						out_graph->offsets_list[v] = current_offset;
						current_offset = next_vertex_offset;
					}

					if(partition + 1 < partitions_count)
						assert(current_offset == partitions_edges[partition + 1]);
					else
						assert(current_offset == out_graph->edges_count);
				}
				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			PTIP("  (3.2) Updating offsets_list");
			exec_info && (exec_info[12] += get_nano_time());	
			mt = exec_info[12];
			PT("(3) Writing edges");
		}

	// Reading PAPI & energy
		tt += get_nano_time();
		printf("%-20s \t\t\t %'.3f (s)\n\n","Total time:", tt/1e9);

		#pragma omp parallel
		{
			assert(0 == thread_papi_read(pe));
		}
		print_hw_events(pe, 1);
		if(exec_info)
		{
			exec_info[0] = tt;
			copy_reset_hw_events(pe, &exec_info[1], 1);
		}

		#ifdef _ENERGY_MEASUREMENT
		{
			struct energy_counters_vals* evals = energy_measurement_stop(em);
			printf("Energy consumption:\n");
			printf("  Packages energy : %'.2f (J)\n", evals->packages_total/1e6);
			printf("  DRAM energy     : %'.2f (J)\n", evals->drams_total/1e6);
			printf("  Total energy    : %'.2f (J)\n", (evals->packages_total + evals->drams_total)/1e6);
			if(exec_info)
			{
				exec_info[18] = evals->packages_total / 1e6;
				exec_info[19] = evals->drams_total / 1e6;
			}

			// energy_measurement_print(evals);
			evals = NULL;
			energy_measurement_release(em);
			em = NULL;
		}
		#endif

	// (4) Sorting
		if(flags & (2U | 1U))
		{
			unsigned long mt = - get_nano_time();
			sort_neighbor_lists(pe, out_graph);
			mt += get_nano_time();

			exec_info && (exec_info[13] = mt);
			// PT("Sorting");
		}
			
	// Validation
		if((flags & 1U))
		{	
			unsigned long mt = - get_nano_time();
			unsigned int tf = 0;
			if(flags & 4U)
				tf = 1U;
			int ret = validate_transposition(pe, in_graph, out_graph, tf);
			if(ret != 1)
			{
				printf("  Validation failed.\n");	
				assert(ret == 1);
			}

			mt += get_nano_time();
			exec_info && (exec_info[14] = mt);
			PT("Validation");
		}

	// Releasing memory
	potra_release: 
		free(partitions);
		partitions = NULL;

		dynamic_partitioning_release(dp);
		dp = NULL;

		free(partitions_edges);
		partitions_edges = NULL;

		free(ttimes);
		ttimes = NULL;

		numa_free(ldv_counters, sizeof(unsigned int) * (1 + in_graph->vertices_count));
		ldv_counters = NULL;

		free(partition2thread);
		partition2thread = NULL;

		if(hash_table != NULL)
		{
			numa_free(hash_table, hash_table_size);
			hash_table = NULL;

			numa_free(RA, sizeof(unsigned int) * in_graph ->vertices_count);
			RA = NULL;

			for(unsigned int t = 0; t < pe->threads_count; t++)
			{
				numa_free(threads_low_counters[t], sizeof(unsigned short) * k);
				threads_low_counters[t] = NULL;
				numa_free(threads_high_counters[t], sizeof(unsigned short) * k);
				threads_high_counters[t] = NULL;
				numa_free(threads_HDV_offsets[t], sizeof(unsigned long) * k);
				threads_HDV_offsets[t] = NULL;
			}

			free(threads_low_counters);
			threads_low_counters = NULL;
			free(threads_high_counters);
			threads_high_counters = NULL;
			free(threads_HDV_offsets);
			threads_HDV_offsets = NULL;
		}
		
	// Finalizing
		printf("\nTransposed graph:");
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