#ifndef __MSF_C
#define __MSF_C

#include "cc.c"

struct sdw_edge
{
	unsigned int source;
	unsigned int dest;
	unsigned int weight;
};

#define sdw_edges_per_page (1024UL * 1024 * 16)

struct msf
{	
	unsigned long vertices_count;
	unsigned long threads_count;
	unsigned long max_pages_count;
	unsigned long total_edges;					// is partially updated during execution, finially in msf_finalize()
	unsigned long total_weight;					// is updated only after msf_finalize()
	unsigned long pages_count;					// used pages, requires atomic access

	struct sdw_edge** pages;
	unsigned long* pages_edges_count;		// number of edges in each page, is updated partially and in msf_finalize()

	unsigned long* threads_last_page_index;		// the index of the last page of each thread
	
	// To prevent successive cacheline invalidations as a result of current adding edges to msf by threads, 
	// we update the pages_edges_count only when the page is fileld, or in the end of execution
	// the other changes are written in threads_last_page_edges that allocates a cacheline 
	// for the counter of each thread
	unsigned long* threads_last_page_edges;
	unsigned long* threads_last_page_edges_not_aligned;

	// Similarly for total weight of edges collected by each thread
	unsigned long* threads_total_weight;
	unsigned long* threads_total_weight_not_aligned;
};

struct msf* msf_alloc(unsigned int vertices_count, unsigned int threads_count)
{
	// Intitial checks
		assert(vertices_count > 0 && threads_count > 0);

	// Allocate mem
		struct msf* ret = calloc(sizeof(struct msf), 1);
		assert(ret != NULL);

	// Initialize vals
		ret->vertices_count = vertices_count;
		ret->threads_count = threads_count;
		ret->max_pages_count = threads_count + 1 + vertices_count / sdw_edges_per_page;
		ret->total_edges = 0;
		ret->total_weight = 0;
		ret->pages_count = 0;		

	// Allocate mem for pages arrays
		ret->pages = calloc(ret->max_pages_count, sizeof(struct sdw_edge*));
		assert(ret->pages != NULL);
		ret->pages_edges_count = calloc(ret->max_pages_count, sizeof(unsigned long));
		assert(ret->pages_edges_count != NULL);

	// Allocate mem for thread vars
		ret->threads_last_page_index = calloc(ret->threads_count, sizeof(unsigned long));
		assert(ret->threads_last_page_index != NULL);

		ret->threads_last_page_edges = calloc( 2 * 64 + (512 / 64) * ret->threads_count, sizeof(unsigned long));
		assert(ret->threads_last_page_edges != NULL);	
		ret->threads_last_page_edges_not_aligned = ret->threads_last_page_edges;	
		// 9-bits alignments 
		if((unsigned long)ret->threads_last_page_edges % 64 != 0)
		{
			unsigned long addr = (unsigned long)ret->threads_last_page_edges;
			ret->threads_last_page_edges = (unsigned long*)(addr + 64 - addr % 64);
		} 

		ret->threads_total_weight = calloc( 2 * 64 + (512 / 64) * ret->threads_count, sizeof(unsigned long));
		assert(ret->threads_total_weight != NULL);
		ret->threads_total_weight_not_aligned = ret->threads_total_weight;	
		// 9-bits alignments 
		if((unsigned long)ret->threads_total_weight % 64 != 0)
		{
			unsigned long addr = (unsigned long)ret->threads_total_weight;
			ret->threads_total_weight = (unsigned long*)(addr + 64 - addr % 64);
		} 

	// Allocate first page
		for(unsigned int tid = 0; tid < ret->threads_count; tid++)
		{
			ret->pages[ret->pages_count] = numa_alloc_interleaved(sizeof(struct sdw_edge)* sdw_edges_per_page);
			assert(ret->pages[ret->pages_count] != NULL);
			ret->threads_last_page_index[tid] = ret->pages_count;
			ret->threads_last_page_edges[tid * 8] = 0;
			ret->threads_total_weight[tid * 8] = 0;
			ret->pages_count++;
		}
	
	return ret;
}

inline void msf_add_edge(struct msf* msf, unsigned int tid, struct sdw_edge* le)
{
	unsigned int t8 = tid * 8;
	if(msf->threads_last_page_edges[t8] == sdw_edges_per_page)
	{
		// Update the edges count of the current page
			msf->pages_edges_count[msf->threads_last_page_index[tid]] = msf->threads_last_page_edges[t8];
		
		// Update the total_edges
			{
				unsigned long prev_val;
				unsigned long new_val;
				do
				{
					prev_val = msf->total_edges;
					new_val = prev_val + sdw_edges_per_page;
				}
				while(__sync_val_compare_and_swap(&msf->total_edges, prev_val, new_val) != prev_val);
			}

		// Allocate a new page index
			unsigned long new_page_index;
			{
				unsigned long new_val;
				do
				{
					new_page_index = msf->pages_count;
					new_val = new_page_index + 1;
				}
				while(__sync_val_compare_and_swap(&msf->pages_count, new_page_index, new_val) != new_page_index);
			}
			assert(new_page_index < msf->max_pages_count);
		
		// Allocate memory for the new page
			msf->pages[new_page_index] = numa_alloc_interleaved(sizeof(struct sdw_edge)* sdw_edges_per_page);
			assert(msf->pages[new_page_index] != NULL);
			msf->threads_last_page_index[tid] = new_page_index;
			msf->threads_last_page_edges[t8] = 0;
	}

	msf->pages[msf->threads_last_page_index[tid]][msf->threads_last_page_edges[t8]].source = le->source;
	msf->pages[msf->threads_last_page_index[tid]][msf->threads_last_page_edges[t8]].dest = le->dest;
	msf->pages[msf->threads_last_page_index[tid]][msf->threads_last_page_edges[t8]].weight = le->weight;
	msf->threads_last_page_edges[t8]++;
	msf->threads_total_weight[t8] += le->weight;

	return;
}

void msf_finalize(struct msf* msf)
{
	for(unsigned int tid = 0; tid < msf->threads_count; tid++)
	{
		msf->total_weight += msf->threads_total_weight[tid * 8];
		msf->total_edges += msf->threads_last_page_edges[tid * 8];
		msf->pages_edges_count[msf->threads_last_page_index[tid]] = msf->threads_last_page_edges[tid * 8];
	}

	return;
}

unsigned long msf_current_edges_count(struct msf* msf)
{
	unsigned long ret = msf->total_edges;
	for(unsigned int tid = 0; tid < msf->threads_count; tid++)
		ret += msf->threads_last_page_edges[tid * 8];
	return ret;
}

void msf_free(struct msf* in)
{
	assert(in != NULL);
	for(unsigned long p = 0; p < in->pages_count; p++)
	{
		assert(in->pages[p] != NULL);
		numa_free(in->pages[p], sizeof(struct sdw_edge) * sdw_edges_per_page);
		in->pages[p] = NULL;
	}

	free(in->threads_last_page_index);
	in->threads_last_page_index = NULL;
	free(in->threads_last_page_edges_not_aligned);
	in->threads_last_page_edges = NULL;
	in->threads_last_page_edges_not_aligned = NULL;
	free(in->threads_total_weight_not_aligned);
	in->threads_total_weight = NULL;
	in->threads_total_weight_not_aligned = NULL;

	free(in->pages_edges_count);
	in->pages_edges_count = NULL;
	free(in->pages);
	in->pages = NULL;

	free(in);
	in = NULL;

	return;
}

struct ll_400_graph* msf2graph(struct par_env* pe, struct msf* msf)
{
	// Initial checks
		assert(pe != NULL && msf != NULL && msf->total_edges < msf->vertices_count);
		printf("\n\033[3;32mmsf2graph\033[0;37m\n");
		unsigned int vertices_count = msf->vertices_count;

	// Allocate memory
		struct ll_400_graph* ret =calloc(sizeof(struct ll_400_graph),1);
		assert(ret != NULL);
		ret->vertices_count = vertices_count;
		ret->edges_count = 2 * msf->total_edges;
		ret->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * ( 1 + ret->vertices_count));
		assert(ret->offsets_list != NULL);
		ret->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * ret->edges_count);
		assert(ret->edges_list != NULL);

	// Vertex partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* partitions = calloc(sizeof(unsigned int), partitions_count+1);
		unsigned long* partitions_edges = calloc(sizeof(unsigned long), partitions_count);
		assert(partitions != NULL && partitions_edges != NULL);
		{
			partitions[0] = 0;
			unsigned int offset = 0 ;
			unsigned int remained = ret->vertices_count;
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned int quota = remained / (partitions_count - p); 
				offset += quota;
				remained -= quota;
				partitions[p+1] = offset;
			}
			assert(remained == 0);
		}

	// Identifying degree of each vertex
		for(unsigned long p = 0; p < msf->pages_count; p++)
		{
			#pragma omp parallel for
			for(unsigned long e = 0; e < msf->pages_edges_count[p]; e++)
			{
				unsigned int src = msf->pages[p][e].source;
				unsigned int dest = msf->pages[p][e].dest;
				assert(src != dest && src < ret->vertices_count && dest < ret->vertices_count);
				
				unsigned long prev_degree;
				unsigned long new_degree;
				do
				{
					prev_degree = ret->offsets_list[src];
					new_degree = prev_degree + 1;
				}
				while(__sync_val_compare_and_swap(&ret->offsets_list[src], prev_degree, new_degree) != prev_degree);

				do
				{
					prev_degree = ret->offsets_list[dest];
					new_degree = prev_degree + 1;
				}
				while(__sync_val_compare_and_swap(&ret->offsets_list[dest], prev_degree, new_degree) != prev_degree);
			}
		}

	// Calculating total edges per partitions
		unsigned int max_degree = 0;
		#pragma omp parallel for reduction(max:max_degree)
		for(unsigned int p = 0; p < partitions_count; p++)
		{
			unsigned long sum = 0;
			for(unsigned int v = partitions[p]; v < partitions[p+1]; v++)
			{
				sum += ret->offsets_list[v];
				if(ret->offsets_list[v] > max_degree)
					max_degree = ret->offsets_list[v];
			}
			partitions_edges[p] = sum;
		}
		printf("Max. degree: %'lu\n",max_degree);

	// Partial sum
		{
			unsigned long sum = 0;
			for(unsigned int p = 0; p < partitions_count; p++)
			{
				unsigned long temp = partitions_edges[p];
				partitions_edges[p] = sum;
				sum += temp;
			}
			assert(sum == ret->edges_count);
		}

	// Updating offsets of vertices
		#pragma omp parallel for
		for(unsigned int p = 0; p < partitions_count; p++)
		{
			unsigned long offset = partitions_edges[p];
			for(unsigned int v = partitions[p]; v < partitions[p+1]; v++)
			{
				unsigned long degree = ret->offsets_list[v];
				ret->offsets_list[v] = offset;
				offset += degree;
			}

			if(p + 1 == partitions_count)
				assert(offset == ret->edges_count);
			else
				assert(offset == partitions_edges[p+1]);
		}
		ret->offsets_list[ret->vertices_count] = ret->edges_count;

	// Writing edges
		for(unsigned long p = 0; p < msf->pages_count; p++)
		{
			#pragma omp parallel for
			for(unsigned long e = 0; e < msf->pages_edges_count[p]; e++)
			{
				unsigned int src = msf->pages[p][e].source;
				unsigned int dest = msf->pages[p][e].dest;
				
				unsigned long prev_offset;
				unsigned long new_offset;
				do
				{
					prev_offset = ret->offsets_list[src];
					new_offset = prev_offset + 1;
				}
				while(__sync_val_compare_and_swap(&ret->offsets_list[src], prev_offset, new_offset) != prev_offset);
				assert(prev_offset < ret->offsets_list[src+1]);
				ret->edges_list[prev_offset] = dest;

				do
				{
					prev_offset = ret->offsets_list[dest];
					new_offset = prev_offset + 1;
				}
				while(__sync_val_compare_and_swap(&ret->offsets_list[dest], prev_offset, new_offset) != prev_offset);
				assert(prev_offset < ret->offsets_list[dest+1]);
				ret->edges_list[prev_offset] = src;
			}
		}

	// Correcting offsets
		#pragma omp parallel for
		for(unsigned int p = 0; p < partitions_count; p++)
		{
			unsigned long offset = partitions_edges[p];
			for(unsigned int v = partitions[p]; v < partitions[p+1]; v++)
			{
				unsigned long next_offset = ret->offsets_list[v];
				ret->offsets_list[v] = offset;
				offset = next_offset;
			}

			if(p + 1 == partitions_count)
				assert(offset == ret->edges_count);
			else
				assert(offset == partitions_edges[p+1]);
		}

	// Sorting neighbour-lists
		#pragma omp parallel for
		for(unsigned int p = 0; p < partitions_count; p++)
			for(unsigned int v = partitions[p]; v < partitions[p+1]; v++)
			{
				unsigned int degree = ret->offsets_list[v+1] - ret->offsets_list[v];
				if(degree < 2)
					continue;
				quick_sort_uint(&ret->edges_list[ret->offsets_list[v]], 0, degree - 1);
			}

	// Validate
		print_ll_400_graph(ret);
		for(unsigned long p = 0; p < msf->pages_count; p++)
		{
			#pragma omp parallel for
			for(unsigned long e = 0; e < msf->pages_edges_count[p]; e++)
			{
				unsigned int src = msf->pages[p][e].source;
				unsigned int dest = msf->pages[p][e].dest;

				assert(-1UL != uint_binary_search(ret->edges_list, ret->offsets_list[dest], ret->offsets_list[dest + 1], src));
				assert(-1UL != uint_binary_search(ret->edges_list, ret->offsets_list[src], ret->offsets_list[src + 1], dest));
			}
		}

	// Releasing memory
		free(partitions);
		partitions = NULL;

		free(partitions_edges);
		partitions_edges = NULL;

	printf("\n");

	return ret;
}

/*
	flags:
		bit 0: print details
*/
int msf_validate(struct par_env* pe, struct ll_400_graph* main_graph, struct msf* forest, unsigned int flags)
{
	// Initial checks
		unsigned long t0 = - get_nano_time();
		assert(pe != NULL && forest != NULL);
		printf("\n\033[3;35mmsf_validate\033[0;37m\n");

	// Check if edges exist in the main graph
		for(unsigned long p = 0; p < forest->pages_count; p++)
		{
			#pragma omp parallel for
			for(unsigned long e = 0; e < forest->pages_edges_count[p]; e++)
			{
				unsigned int src = forest->pages[p][e].source;
				unsigned int dest = forest->pages[p][e].dest;
				assert(-1UL != uint_binary_search(main_graph->edges_list, main_graph->offsets_list[dest], main_graph->offsets_list[dest + 1], src));
				assert(-1UL != uint_binary_search(main_graph->edges_list, main_graph->offsets_list[src], main_graph->offsets_list[src + 1], dest));
			}
		}
		printf("\033[3;35m(1) Edges are valid\033[0;37m.\n");

	// Create a ll_400_graph from forest
		struct ll_400_graph* fg = msf2graph(pe, forest);

	// Check if the main_graph and fg, i.e. msf, have the same connectivity	for each vertex
		// proof of connectivity correctness is similar to correctness of CCs in XP124
		unsigned int main_ccs_count = 0;
		unsigned int* main_cc = cc_thrifty_400(pe, main_graph, 0, NULL, &main_ccs_count);
		
		unsigned int forest_ccs_count = 0;
		unsigned int* forest_cc = cc_thrifty_400(pe, fg, 0, NULL, &forest_ccs_count);

		#pragma omp parallel for 
		for(unsigned v = 0; v < main_graph->vertices_count; v++)
			for(unsigned long e = main_graph->offsets_list[v]; e < fg->offsets_list[v+1]; e++)
				assert(forest_cc[v] == forest_cc[main_graph->edges_list[e]]);

		assert(main_ccs_count == forest_ccs_count);
		assert(fg->edges_count / 2 + forest_ccs_count  == forest->vertices_count);
		printf("\033[3;35m(2) Connectivity is correct.\033[0;37m\n");

		cc_release(main_graph, main_cc);
		main_cc = NULL;
		
	// Check there is no cycle in the fg
		if(flags & 1U)
			printf("\n");
		// Memory allocation for worklists and parents array
			unsigned long tile_size = 1024;
			unsigned long worklist_size = fg->vertices_count; // it should be at least |V|/3 + 1
			unsigned long* worklist = numa_alloc_interleaved(sizeof(unsigned long) * 3 * worklist_size);
			unsigned long* next_worklist = numa_alloc_interleaved(sizeof(unsigned long) * 3 * worklist_size);
			assert(worklist != NULL && next_worklist != NULL);
			
			unsigned long worklist_count = 0UL;
			unsigned long next_worklist_count = 0UL;

			// We use this array to store parent of each vertex in the forest
			// We store parent_id + 1 as parent of each vertex to be able to use 0 as not previously accessed vertices 
			unsigned int* parents = numa_alloc_interleaved(sizeof(unsigned int) * fg->vertices_count);
			assert(parents != NULL);

		// Finding max-degree of each component and using them as BFS start points 
			{
				unsigned long* all_vals = next_worklist; // temporarily

				// Finding max-degrees
				#pragma omp parallel for 
				for(unsigned v = 0; v < fg->vertices_count; v++)
				{
					unsigned int my_vals[2];
					my_vals[1] = v;
					my_vals[0] = fg->offsets_list[v + 1] - fg->offsets_list[v];

					unsigned int component = forest_cc[v];

					while(1)
					{
						unsigned long prev_val = all_vals[component];
						if((unsigned int)prev_val >= my_vals[0])
							break;
						__sync_val_compare_and_swap(&all_vals[component], prev_val, *(unsigned long*)my_vals);
					}
				}

				// Add max-degrees to the worklist
				#pragma omp parallel for 
				for(unsigned c = 0; c <= fg->vertices_count; c++)
				{
					if(all_vals[c] == 0)
						continue;

					unsigned int vid = (all_vals[c] >> 32);
					unsigned int degree = (unsigned int)all_vals[c];
					// assert(fg->offsets_list[vid + 1] - fg->offsets_list[vid] == degree);
					if(degree == 0)
						continue;					
					if(c == 0) 
						printf("Component: %'u \t Max-Degree: %'u \t for vertex: %'u \n", c, degree, vid);

					parents[vid] = 0;

					for(unsigned long e = fg->offsets_list[vid]; e < fg->offsets_list[vid + 1]; e += tile_size)
					{
						unsigned long next_e= min(e + tile_size, fg->offsets_list[vid + 1]);

						unsigned long wl_index;
						unsigned long wl_next_index;
						do
						{
							wl_index = worklist_count;
							wl_next_index = wl_index + 1;
						}
						while(__sync_val_compare_and_swap(&worklist_count, wl_index, wl_next_index) != wl_index);
						assert(wl_index < worklist_size);

						worklist[3 * wl_index] = vid;
						worklist[3 * wl_index + 1] = e;
						worklist[3 * wl_index + 2] = next_e;
					}
				}

				all_vals = NULL;
			}

		// BFS in push direction to check if there is a cycle  
			unsigned int iter = 0;
			while(worklist_count)
			{
				if(flags & 1U)
					printf("Push iter %'5u \t\t |worklist|: %'u\n", iter++, worklist_count);
				unsigned int ret = 1;
				#pragma omp parallel for reduction(min:ret) 
				for(unsigned vi = 0; vi < worklist_count; vi++)
				{
					unsigned int v = worklist[3 * vi];
					unsigned long start_offset = worklist[3 * vi + 1];
					unsigned long end_offset = worklist[3 * vi + 2];

					unsigned int parent = parents[v] - 1;
					for(unsigned long e = start_offset; e < end_offset; e++)
					{
						unsigned int neighbour = fg->edges_list[e];
						if(neighbour == parent)
							continue;

						unsigned int prev_par = __sync_val_compare_and_swap(&parents[neighbour], 0, v + 1);
						if(prev_par != 0)
						{
							printf("Cycle exists to access %u from %u and %u.\n", neighbour, v, prev_par - 1);
							assert(prev_par == 0);
							ret = 0;
						}

						// add neighbour to next_worklist
						for(unsigned long e2 = fg->offsets_list[neighbour]; e2 < fg->offsets_list[neighbour + 1]; e2 += tile_size)
						{
							unsigned long next_e2= min(e2 + tile_size, fg->offsets_list[neighbour + 1]);

							unsigned long wl_index;
							unsigned long wl_next_index;
							do
							{
								wl_index = next_worklist_count;
								wl_next_index = wl_index + 1;
							}
							while(__sync_val_compare_and_swap(&next_worklist_count, wl_index, wl_next_index) != wl_index);
							assert(wl_index < worklist_size);

							next_worklist[3 * wl_index] = neighbour;
							next_worklist[3 * wl_index + 1] = e2;
							next_worklist[3 * wl_index + 2] = next_e2;
						}
					}
				}

				if(ret == 0)
					return 0;


				// swapping
				{
					unsigned long* temp = next_worklist;
					next_worklist = worklist;
					worklist = temp;

					worklist_count = next_worklist_count;
					next_worklist_count = 0UL;
				}
			}
		// Releasing memory
			numa_free(worklist, sizeof(unsigned long) * 3 * worklist_size);
			numa_free(next_worklist, sizeof(unsigned long) * 3 * worklist_size);
			worklist = NULL;
			next_worklist = NULL;

			numa_free(parents, sizeof(unsigned int) * fg->vertices_count);
			parents = NULL;

		// Finalizing
			printf("\033[3;35m(3) No cycle found in the forest.\033[0;37m\n");

	// Release memory
		cc_release(fg, forest_cc);
		forest_cc = NULL;

		release_numa_interleaved_ll_400_graph(fg);
		fg = NULL;

	// Finalizing
		t0 += get_nano_time();
		printf("Exec. time: \t\t %'.1f (ms) \n", t0 / 1e6);

	return 1;
}

/*
	Serial Prim
		This implementation alters the topology by removing intra-component edges in each traversal of neighbours
		flags: 
			bit 0: print each edge
*/
struct msf* msf_prim_serial(struct par_env* pe, struct ll_404_graph* g, unsigned int flags)
{
	// Initial checks
		assert(g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;33mprim_serial\033[0;37m\n");

	// Memory allocation 
		struct msf* forest = msf_alloc(g->vertices_count, pe->threads_count);

		// vertex component 
		unsigned int* component = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(component != NULL);
		for(unsigned int v=0; v<g->vertices_count; v++)
			component[v] = v;

		// visited vertices
		unsigned int* vv = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(vv != NULL);

	// Let's go
		unsigned long weights_sum = 0;
		unsigned int cc_count = 0;
		unsigned int max_degree = 0;
		for(unsigned int v = 0; v < g->vertices_count; v++)
		{
			if(g->offsets_list[v+1] - g->offsets_list[v] > max_degree)
				max_degree = g->offsets_list[v+1] - g->offsets_list[v];

			// has been processed before
			if(component[v] != v)
				continue;

			cc_count++;
			struct sdw_edge le = {0};

			unsigned int vv_count = 0;
			vv[vv_count++] = v;

			while(1)
			{
				le.weight = -1U;
				for(unsigned int vindex = 0; vindex < vv_count; vindex++)
				{
					unsigned int v2 = vv[vindex];
					unsigned long write_offset = g->offsets_list[v2];
					for(unsigned long e = g->offsets_list[v2]; e < g->offsets_list[v2 + 1]; e++)
					{
						unsigned int dest = g->edges_list[2 * e];
						unsigned int weight = g->edges_list[2 * e + 1];

						if(dest == -1U)
							continue;

						// not a symmetric input graph
						if(component[dest] < v)
						{
							assert(0 && "graph is not symmetric");
							exit(-1);
						}

						// intra-component 
						if(component[dest] == v)
							continue;

						g->edges_list[2 * write_offset] = dest;
						g->edges_list[2 * write_offset + 1] = weight;
						write_offset++;

						if(weight < le.weight)
						{
							le.weight = weight;
							le.dest = dest;
							le.source = v2;
						}
					}

					if(g->offsets_list[v2] != g->offsets_list[v2 + 1] && write_offset != g->offsets_list[v2 + 1])
						g->edges_list[2 * write_offset] = -1U;
				}

				if(le.weight == -1U)
				{
					// printf("#%u: No edge found\n", v);
					break;
				}

				// add the lightest edge to the forest
				msf_add_edge(forest, 0, &le);

				// add the lightest destination to the current component
				component[le.dest] = v;

				// add the lightest destination to vertices for edge inspection
				vv[vv_count++] = le.dest;

				weights_sum += le.weight;

				if(flags & 1U)
					printf("# c:%-10u s: %-10u d: %-10u w: %u\n", v, le.source, le.dest, le.weight);
			}
		}
		printf("Main graph max. degree: %'u\n", max_degree);
		msf_finalize(forest);

	// Free mem
		numa_free(component, sizeof(unsigned int) * g->vertices_count);
		component = NULL;

		numa_free(vv, sizeof(unsigned int) * g->vertices_count);
		vv = NULL;

	// Report
		t0 += get_nano_time();
		printf("Exec. time: \t\t %'.1f (ms) \n", t0 / 1e6);
		printf("|CCs|: %'u\n", cc_count);
		printf("Forest weight: \033[3;33m%'lu\033[0;37m for \033[3;33m%'lu\033[0;37m edges.\n", forest->total_weight, forest->total_edges);
		assert(cc_count + forest->total_edges == g->vertices_count);

	return forest;
}

struct sdw_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	struct sdw_edge* edges_list;
	unsigned long offsets_list_size;
	unsigned long edges_list_size;
};


#define copy_sdw_edge(_from, _to) \
	{ \
		_to.source = _from.source; \
		_to.dest = _from.dest; \
		_to.weight = _from.weight; \
	}

#define edges_chunk (1024UL * 1024)

struct edge_page
{
	struct edge_page* next_page;
	struct sdw_edge edges[edges_chunk];
};

struct edge_storage
{
	unsigned long current_page_count;
	struct edge_page* first_page;
	struct edge_page* current_page;
	void* not_aligned_mem;
};

struct edge_storage* edge_storage_initialize()
{
	void* not_aligned_mem = calloc(2 + ceil(sizeof(struct edge_storage)/64), 64);
	assert(not_aligned_mem != NULL);
	struct edge_storage* ret = not_aligned_mem;

	// Assure the cacheline is only used by this struct
	if((unsigned long)ret % 64 != 0)
	{
		unsigned long addr = (unsigned long)ret;
		ret = (struct edge_storage*)(addr + 64 - addr % 64);
	}
	ret->not_aligned_mem = not_aligned_mem;

	ret->current_page_count = 0;
	ret->first_page = numa_alloc_interleaved(sizeof(struct edge_page));
	assert(ret->first_page != NULL);
	ret->first_page->next_page = NULL;
	ret->current_page = ret->first_page;

	return ret;	
}

void edge_storage_free(struct edge_storage* es)
{
	assert(es != NULL);

	while(es->first_page != NULL)
	{
		struct edge_page* page = es->first_page;
		es->first_page = es->first_page->next_page;
		numa_free(page, sizeof(struct edge_page));
		page = NULL;
	}

	es->current_page = NULL;
	es->current_page_count = 0;

	free(es->not_aligned_mem);
	es = NULL;

	return;
}

void edge_storage_reset(struct edge_storage* es)
{
	es->current_page = es->first_page;
	es->current_page_count = 0;
	return;
}

inline struct sdw_edge* edge_storage_get_one(struct edge_storage* es)
{
	struct sdw_edge* ret = &es->current_page->edges[es->current_page_count];
	es->current_page_count++;

	if(es->current_page_count == edges_chunk)
	{
		if(es->current_page->next_page == NULL)
		{
			es->current_page->next_page = numa_alloc_interleaved(sizeof(struct edge_page));
			assert(es->current_page->next_page != NULL);
			es->current_page->next_page->next_page = NULL;
		}
		es->current_page = es->current_page->next_page;
		es->current_page_count = 0;
	}

	return ret;
}

/*
	MASTIFF: Structure-Aware Minimum Spanning Tree/Forest (MST/MSF)
	https://blogs.qub.ac.uk/DIPSA/mastiff-structure-aware-minimum-spanning-tree-forest/
	
	@INPROCEEDINGS{10.1145/3524059.3532365,
		author = {Koohi Esfahani, Mohsen and Kilpatrick, Peter and Vandierendonck, Hans},
		title = {{MASTIFF}: Structure-Aware Minimum Spanning Tree/Forest},
		year = {2022},
		isbn = {},
		publisher = {Association for Computing Machinery},
		address = {New York, NY, USA},
		url = {https://doi.org/10.1145/3524059.3532365},
		doi = {10.1145/3524059.3532365},
		booktitle = {Proceedings of the 36th ACM International Conference on Supercomputing},
		numpages = {13}
	}

	Arguments:
		
		g:
			The weighted graph is received as `g`, and t is necessary to not have any self edges (loops) or repeated edges in the input graph `g`.

		flags: 
			bit-0: print details

		exec_info: 
			if not NULL, will have 
				[0]: exec time
				[1-7]: papi events
				[8]: #iterations
*/

struct msf* msf_mastiff(struct par_env* pe, struct ll_404_graph* g, unsigned long* exec_info, unsigned int flags)
{
	// Initial checks
		assert(g != NULL);
		unsigned long t0 = - get_nano_time();
		printf("\n\033[3;34mmsf_mastiff\033[0;37m\n");

	// Reset papi
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			papi_reset(pe->papi_args[tid]);
		}

	// Edge partitioning
		unsigned int thread_partitions = 64;
		unsigned int partitions_count = pe->threads_count * thread_partitions;
		unsigned int* edge_partitions = calloc(sizeof(unsigned int), partitions_count+1);
		assert(edge_partitions != NULL);
		struct dynamic_partitioning* dp = dynamic_partitioning_initialize(pe, partitions_count);
		parallel_edge_partitioning((struct ll_400_graph*)g, edge_partitions, partitions_count); 

	// Memory allocation 
		struct msf* forest = msf_alloc(g->vertices_count, pe->threads_count);
		unsigned int iter = 0;

		// The sdw_edge storage per thread: these are used to facilitate atomically storing lightests edges of each component
		// each edge has a size of 12B, so we cannot use __sync_val_compare_and_swap  to store edges.
		// To solve that we store the pointer to each edge atomically and in each iteration we reset these pointers
		// We do not garbage collect these edges until the next iteration that are reused with new values
		struct edge_storage** edge_storages = calloc(sizeof(struct edge_storage*), pe->threads_count);
		assert(edge_storages != NULL);
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			edge_storages[tid] = edge_storage_initialize();
		}

		unsigned long* ttimes = calloc(sizeof(unsigned long), pe->threads_count);
		assert(ttimes != NULL);

		// Stores the vertex to which the vertex has been merged
		unsigned int* parent =  numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		// Stores the lightest edge per vertex
		struct sdw_edge** lightests =  numa_alloc_interleaved(sizeof(struct sdw_edge*) * ( 1 + g->vertices_count));
		assert(parent != NULL && lightests != NULL);

		// Vertex Statuses 
		unsigned char ROOT = 1, MERGED = 2, EXEMPT = 3;
		unsigned char* status =  numa_alloc_interleaved(sizeof(unsigned char) * g->vertices_count);
		assert(status != NULL);

		// Component Size: holds the number of vertices merged to a vertex
		unsigned int* cs = numa_alloc_interleaved(sizeof(unsigned int) * g->vertices_count);
		assert(cs != NULL);
				
		// Root Vertices Count
		unsigned int rv_count = 0;

	// (Pre-1) Identifying main graph's components assigning ID of max degree of each component as label for all vertices
		unsigned int* graph_component = NULL;
		unsigned long mt = - get_nano_time();
		{

			if(g->edges_count > 5 * g->vertices_count)
				graph_component = cc_thrifty_404(pe, g, 2U, NULL, NULL);
			else
			{
				// JT CC
				graph_component = numa_alloc_interleaved(sizeof(unsigned int)* g->vertices_count);

				#pragma omp parallel for  
				for(unsigned long v=0; v < g->vertices_count; v++)
					graph_component[v] = v;

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

						for(unsigned int v = edge_partitions[partition]; v < edge_partitions[partition + 1]; v++)
							for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
							{
								unsigned int neighbour = g->edges_list[2 * e];
								// if(neighbour >= v)
									// break;

								unsigned int x = v;
								unsigned int y = neighbour;

								while(1)
								{
									// find(x)
									while(x != graph_component[x])
										x = graph_component[x];

									// find(y)
									while(y != graph_component[y])
										y = graph_component[y];

									if(x == y)
										break;

									if(x < y)
									{
										if(__sync_bool_compare_and_swap(&graph_component[y], y, x))
											break;
									}
									else
									{
										if(__sync_bool_compare_and_swap(&graph_component[x], x, y))
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
					PTIP("    (JT main loop)");

				// Compress paths
				#pragma omp parallel for  
				for(unsigned long v=0; v < g->vertices_count; v++)
					while(graph_component[graph_component[v]] != graph_component[v])
						graph_component[v] = graph_component[graph_component[v]];

				// Validation of JT
					// #pragma omp parallel for  
					// for(unsigned long v=0; v < g->vertices_count; v++)
					// 	assert(graph_component2[v] == graph_component2[graph_component[v]]);
			}

			// Temporarily we use lightests as an array
			unsigned long* max_degrees = (unsigned long*)lightests;

			// Update max_degrees 
			#pragma omp parallel for  
			for(unsigned long v=0; v < g->vertices_count; v++)
			{
				unsigned long degree = g->offsets_list[v+1] - g->offsets_list[v];
				unsigned long degree_id = (v << 32) + degree;

				while(1)
				{
					unsigned long prev_val = max_degrees[graph_component[v]];
					if( prev_val && (degree <= (unsigned int)prev_val) )
							break;
					if(__sync_val_compare_and_swap(&max_degrees[graph_component[v]], prev_val, degree_id) == prev_val)
						break;
				}
			}

			// Update graph_component with ID of the vertex with max degree on their component
			#pragma omp parallel for  
			for(unsigned int v=0; v < g->vertices_count; v++)
			{
				unsigned long degree_id = max_degrees[graph_component[v]];
				graph_component[v] =  (unsigned int)(degree_id >> 32);
			}
			
			max_degrees = NULL;			
		}
		mt += get_nano_time();
		if(flags & 1U)
			PT("\033[3;34mPre-1\033[0;37m: Identifying graph componets");
		
	// (Pre-2) Initializing parent, cs, rv_count
		mt = - get_nano_time();
		unsigned int graph_ccs = 0;
		#pragma omp parallel for  reduction(+:graph_ccs)
		for(unsigned int v=0; v < g->vertices_count; v++)
		{
			parent[v] = v;
			cs[v] = 1;

			if(graph_component[v] == v)
			{
				// vertex that is the max-degree in the component
				graph_ccs++;

				status[v] = EXEMPT;
				
				// this vertex/component does not try to join to other vertices
				lightests[v] = NULL;
			}
			else
			{
				status[v] = ROOT;

				// We do not need to set the lightests as it is initialized in the execution of the first step of the first iteration
				// lightests[v] = NULL;	
			}
		}

		rv_count = g->vertices_count;

		mt += get_nano_time();
		if(flags & 1U)
		{
			char temp[128];
			sprintf(temp, "\033[3;34mPre-2\033[0;37m: Initializing vars; |G.CCs|= %'u; |RV|= %'u", graph_ccs, rv_count);
			PT(temp);
		}

	// Iterations
	while(rv_count > graph_ccs)
	{
		unsigned long iter_time = - get_nano_time();

		// (1) Finding the lightests edges of active vertex
			mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();
				
				// Reset edge storage of this thread
				edge_storage_reset(edge_storages[tid]);
				// thread lightest edge
				struct sdw_edge* tle= edge_storage_get_one(edge_storages[tid]);

				if(iter == 0)
				{
					unsigned int partition = -1U;
					while(1)
					{
						partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
						if(partition == -1U)
							break; 

						for(unsigned int v = edge_partitions[partition]; v < edge_partitions[partition + 1]; v++)
						{
							// Inactive vertex
							if(status[v] != ROOT)
								continue;

							tle->source = v;
							tle->weight = -1U;
							tle->dest = -1U;

							for(unsigned long e = g->offsets_list[v]; e < g->offsets_list[v + 1]; e++)
							{
								unsigned int dest = g->edges_list[2 * e];
								unsigned int weight = g->edges_list[2 * e + 1];

								// Prevent the same-weight cycles to be added to the forest when edges do not have unique weights:
								// Select the lightest edge to the neighbour with lowest ID, i.e., component								
								if( weight < tle->weight || ( weight == tle->weight && dest < tle->dest) )
								{
									tle->dest = dest;
									tle->weight = weight;
								}
							}

							// Write the tle to lightests
							lightests[v] = tle;
							tle = edge_storage_get_one(edge_storages[tid]);

							// printf("v:%2u le-dest:%2u le-weight:%2u\n", v, lightests[v]->dest, lightests[v]->weight);
						}
					}
				}
				else
				{
					unsigned int partition = -1U;
					while(1)
					{
						partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
						if(partition == -1U)
							break; 

						for(unsigned int v = edge_partitions[partition]; v < edge_partitions[partition + 1]; v++)
						{
							unsigned int my_parent = parent[v];
							if(status[my_parent] != ROOT)
								continue;
							
							tle->source = v;
							tle->weight = -1U;
							tle->dest = -1U;
							unsigned int parent_tle_dest = -1U;

							// unsigned long write_offset = g->offsets_list[v];
							unsigned long e = g->offsets_list[v];
							for(; e < g->offsets_list[v + 1]; e++)
							{
								unsigned int dest = g->edges_list[2 * e];
								
								// no more edge
								// if(dest == -1U)
								// 	break;

								unsigned int weight = g->edges_list[2 * e + 1];

								// An intra-component edge
								if(parent[dest] == my_parent)
									continue;

								// Prevent the same-weight cycles problem
								if( weight < tle->weight || ( weight == tle->weight && parent[dest] < parent_tle_dest ) )
								{
									tle->dest = dest;
									tle->weight = weight;
									parent_tle_dest = parent[dest];
								}

								// Rewriting edgelist 
								// if(e != write_offset)
								// {
								// 	g->edges_list[2 * write_offset] = dest;
								// 	g->edges_list[2 * write_offset + 1] = weight;
								// }
								// write_offset++;
							}

							// if(e > write_offset + 1)
							// 	g->edges_list[2 * write_offset] = -1U;

							// This vertex has not had any outgoing edges
							if(tle->dest == -1U)
								continue;

							// Update the lightest edge of this component
							while(1)
							{
								struct sdw_edge* current = lightests[my_parent];
								if(current)
								{
									if(current->weight < tle->weight)
										break;
									if(current->weight == tle->weight && parent[current->dest] <= parent_tle_dest)
										break;
								}

								if(__sync_val_compare_and_swap(&lightests[my_parent], current, tle) == current)
								{
									tle = edge_storage_get_one(edge_storages[tid]);
									break;
								}
							}

							// printf("v:%2u fc: %2u; le-dest:%2u le-weight:%2u\n", v, my_parent, lightests[my_parent]->dest, lightests[my_parent]->weight);
						}
					}
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			dynamic_partitioning_reset(dp);
			if(flags & 1U)
				PTIP("    (1) Selecting the lightests");

		// (2) Removing symmetric selected edges (with the same source and destination components)
		 	mt = - get_nano_time();
		 	unsigned int sym_edges = 0;
			#pragma omp parallel reduction(+:sym_edges)
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();

				#pragma omp for nowait
				for(unsigned int v = 0; v < g->vertices_count; v++)
				{
					if(status[v] != ROOT)
						continue;

					struct sdw_edge* edge = lightests[v];
					// inactive component
					if(edge == NULL)
						continue;

					unsigned int dest = parent[edge->dest];
					
					struct sdw_edge* dest_edge = lightests[dest];
					if(dest_edge == NULL)
						continue;
					
					unsigned int dest_dest = parent[dest_edge->dest];
					if(dest_dest == v && v > dest)
					{
						lightests[v] = NULL;
						sym_edges++;
					}
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
			{
				char temp[128];
				sprintf(temp, "    (2) Removing symmetric selected edges (count: %'u)", sym_edges);
				PTIP(temp);
			}

		// (3) Adding new edges to the forest and updating its forest component based on the new edges
		 	mt = - get_nano_time();
			#pragma omp parallel  
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();

				#pragma omp for nowait
				for(unsigned int v = 0; v < g->vertices_count; v++)
				{
					if(status[v] != ROOT)
						continue;

					struct sdw_edge* edge = lightests[v];
					// a symmetric edge
					if(edge == NULL)
						continue;

					msf_add_edge(forest, tid, lightests[v]);

					// reset lightests for the next iteration
					lightests[v] = NULL;

					parent[v] = parent[edge->dest];
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
				PTIP("    (3) Adding edges to the forest");

		// (4) Updating parents and variables 
			mt = - get_nano_time();
			unsigned int merged_vertices = 0;
			#pragma omp parallel  reduction(+:merged_vertices)
			{
				unsigned tid = omp_get_thread_num();
				ttimes[tid] = - get_nano_time();

				#pragma omp for nowait
				for(unsigned int v = 0; v < g->vertices_count; v++)
				{

					if(status[v] != ROOT && status[parent[v]] == EXEMPT)
						continue;

					unsigned int my_parent = parent[v];
					
					// Reach the topest parent
					while(my_parent != parent[my_parent])
						my_parent = parent[my_parent];

					// Update all parents
					unsigned int temp = v;
					while(parent[temp] != my_parent)
					{
						unsigned int temp_parent = parent[temp];
						parent[temp] = my_parent;
						temp = temp_parent;
					}

					// For the recently merged vertices
					if(my_parent != v && status[v] == ROOT)
					{
						status[v] = MERGED;
						merged_vertices++;
						__atomic_fetch_add(&cs[my_parent], cs[v], __ATOMIC_RELAXED);
					}
				}

				ttimes[tid] += get_nano_time();
			}
			mt += get_nano_time();
			if(flags & 1U)
			{
				char temp[128];
				sprintf(temp, "    (4) Updating parents (merged_vertices: %'u)", merged_vertices);
				PTIP(temp);
			}

			assert(merged_vertices + sym_edges == rv_count - graph_ccs);
			rv_count -= merged_vertices;

		// (5) Updating the exempted components
			// if(0)
			{
				// We temporarily use lightests as `svc_id` array to find the active vertex with the maximum sub-vertices in each graph component 
				unsigned long* svc_ids = (unsigned long*)lightests;
				mt = - get_nano_time();
				#pragma omp parallel
				{
					unsigned tid = omp_get_thread_num();
					ttimes[tid] = - get_nano_time();

					// Find the active vertex with max cs
					#pragma omp for
					for(unsigned long v = 0; v < g->vertices_count; v++)
					{
						// assert(svc_id[v] == 0UL);

						if(status[v] == MERGED)
							continue;

						if(status[v] == EXEMPT)
						{
							// printf("v: %u, cs[v]: %'u  (gc[v]: %'u)\n", v, cs[v], graph_component[v]);
							status[v] = ROOT;
						}

						unsigned int gc_v = graph_component[v];
						unsigned int cs_v = cs[v];
						unsigned long svc_id = (v << 32) + cs_v;

						while(1)
						{
							unsigned long prev_val = svc_ids[gc_v];
							if( prev_val && (cs_v <= (unsigned int)prev_val) )
									break;
							if(__sync_val_compare_and_swap(&svc_ids[gc_v], prev_val, svc_id) == prev_val)
								break;
						}
					}

					// Set the vertex with max cs as EXEMPT
					#pragma omp for
					for(unsigned int c = 0; c < g->vertices_count; c++)
					{
						if(!svc_ids[c])
							continue;

						unsigned int vertex_id = (unsigned int)(svc_ids[c] >> 32);
						status[vertex_id] = EXEMPT;

						// printf("v: %u, cs[v]: %'u  (gc[v]: %'u)\n", vertex_id, cs[vertex_id], graph_component[vertex_id]);

						svc_ids[c] = 0UL;
					}

					ttimes[tid] += get_nano_time();
				}

				svc_ids = NULL;

				mt += get_nano_time();
				if(flags & 1U)
					PTIP("    (5) Updating convergance ");
			}

		// Timing
			iter_time += get_nano_time();
			unsigned long forest_edges = msf_current_edges_count(forest);
			if(flags & 1U)
			{
				double fe_percent = 100.0 * forest_edges / (forest->vertices_count - graph_ccs);
				double av_percent = 100.0 * rv_count / g->vertices_count;
				printf("\033[3;34mIt-%u\033[0;37m; time(ms):  %'10.1f ;   |RV|: %'10u (%4.1f%%) ;  Frst.|E|: %'lu (%4.1f%%);\n", 
					iter, iter_time/1e6, rv_count, av_percent, forest_edges, fe_percent);
			}
			iter++;
	}

	msf_finalize(forest);

	// Free mem
		numa_free(status, sizeof(unsigned char) * g->vertices_count);
		status = NULL;
		numa_free(cs, sizeof(unsigned int) * g->vertices_count);
		cs = NULL;
		
		cc_release((struct ll_400_graph*)g, graph_component);
		graph_component = NULL;
		numa_free(parent, sizeof(unsigned int) * g->vertices_count);
		parent = NULL;
		numa_free(lightests, sizeof(struct sdw_edge*) * (1 + g->vertices_count));
		lightests = NULL;	

		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();
			edge_storage_free(edge_storages[tid]);
			edge_storages[tid] = NULL;
		}
		free(edge_storages);
		edge_storages = NULL;

		free(ttimes);
		ttimes = NULL;
		free(edge_partitions);
		edge_partitions = NULL;
		dynamic_partitioning_release(dp);
		dp = NULL;

	// Saving events
		#pragma omp parallel
		{
			assert(0 == thread_papi_read(pe));
		}
		if(flags & 1U)
			print_hw_events(pe, 1);
		if(exec_info)
		{
			copy_reset_hw_events(pe, &exec_info[1], 1);
			exec_info[8] = iter;
		}
		
	// Report
		t0 += get_nano_time();
		printf("Exec. time: \t\t %'.1f (ms) \n", t0 / 1e6);
		// printf("|CCs|: %'u\n", cc_count);
		printf("Forest weight: \033[3;34m%'lu\033[0;37m for \033[3;34m%'lu\033[0;37m edges.\n", forest->total_weight, forest->total_edges);
		if(exec_info)
			exec_info[0] = t0;

	return forest;	
}

#endif
