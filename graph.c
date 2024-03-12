#ifndef __GRAPH_C
#define __GRAPH_C

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
#include <mqueue.h>
#include <time.h>
#include <pthread.h>

#include "omp.c"
#include "paragrapher.h"

// 4 Bytes ID per vertex, without weights on edges or vertices
struct ll_400_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	unsigned int* edges_list;
};

// 4 Bytes ID per vertex, 4 Bytes weight per edge, with no weight on vertices
// Each edge has two unsigned int elements in the edges_list, 
// The first one is the destination/source, and 
// The second one is the weight
struct ll_404_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	unsigned int* edges_list;
};

// 8 Bytes IDs per vertex, without weights on edges or vertices
struct ll_800_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	unsigned long* edges_list;
};

void print_ll_400_graph(struct ll_400_graph* ret)
{
	printf("\n|V|: %'20lu\n|E|: %'20lu\n", ret->vertices_count, ret->edges_count);
	printf("First offsets: ");
	for(unsigned int v=0; v<min(ret->vertices_count + 1, 20); v++)
		printf("%lu, ", ret->offsets_list[v]);
	if(ret->vertices_count > 20)
	{
		printf("...\nLast offsets: ... ");
		for(unsigned int v = ret->vertices_count - 20; v <= ret->vertices_count; v++)
			printf(", %lu", ret->offsets_list[v]);
	}

	if(ret->edges_list)
	{
		printf("\nFirst edges: ");
		for(unsigned long e=0; e<min(ret->edges_count, 20); e++)
			printf("%u, ", ret->edges_list[e]);
		if(ret->edges_count > 20)
		{
			printf(" ...\nLast edges: ... ");
			for(unsigned long e = ret->edges_count - 20; e < ret->edges_count; e++)
				printf(", %u", ret->edges_list[e]);
		}
	}

	printf("\n\n");

	return;
}

struct ll_400_graph* get_ll_400_txt_graph(char* file_name)
{
	// Check if file exists
		if(access(file_name, F_OK) != 0)
		{
			printf("Error: file \"%s\" does not exist\n",file_name);
			return NULL;
		}

	// Reading vertices and edges count and graph size
		unsigned long vertices_count = 0;
		unsigned long edges_count = 0;	
		{
			char temp[512];
			sprintf(temp, "head -n1 %s", file_name);
			FILE *fp = popen(temp, "r");
			fscanf(fp, "%lu", &vertices_count);
			pclose(fp);
			printf("Vertices: %'lu\n",vertices_count);

			sprintf(temp, "head -n2 %s | tail -n1", file_name);
			fp = popen(temp, "r");
			fscanf(fp, "%lu", &edges_count);
			pclose(fp);
			printf("Edges: %'lu\n",edges_count);
		}

	// Allocate memory
		struct ll_400_graph* g =calloc(sizeof(struct ll_400_graph),1);
		assert(g != NULL);
		g->vertices_count = vertices_count;
		g->edges_count = edges_count;
		g->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * (1 + g->vertices_count));
		assert(g->offsets_list != NULL);
		g->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * g->edges_count);
		assert(g->edges_list != NULL);
		
	// Reading graph from disk
	{
		int fd=open(file_name, O_RDONLY | O_DIRECT);
		if(fd<0)
		{
			printf("Can't open the file: %d - %s\n",errno,strerror(errno));
			return NULL;
		}

		unsigned long buf_size=4096UL * 1024 * 24;
		char* buf=malloc(sizeof(char)*buf_size);
		assert(buf != NULL);
		char* main_buff_address = buf;
		
		// 4096 alignment of buf for O_DIRECT
		if((unsigned long)buf % 4096)
		{
			unsigned long excess = 4096 - (unsigned long)buf % 4096 ;
			buf = (char*)((unsigned long)buf + excess);
			buf_size -= 4096;
 		}

		long count=-1;

		unsigned long val=0;
		unsigned int val_length=0;
		unsigned int status=0;

		unsigned long t1=get_nano_time();
		unsigned long vl_count=0;
		unsigned long el_count=0;
		unsigned long total_read_bytes = 0;

		while((count=read(fd, buf, buf_size)) > 0)
		{
			total_read_bytes += count;
			int i=0;

			while(i<count)
			{
				//if(i<50) printf("status: %d, v: %c\n",status,buf[i]);
				if(buf[i]!='\n' && buf[i]!=' ')
				{
					val = val * 10 + (buf[i]-'0');
					val_length++;
				}
				else if(val_length)
				{
					switch(status)
					{
						case 0:
							assert(vertices_count == val);
							break;

						case 1:
							assert(edges_count == val);
							break;

						case 2:
							g->offsets_list[vl_count++]=val;
							break;

						case 3:
							assert(val < (1UL<<32));
							g->edges_list[el_count++]=val;
							break;
					}
					val=0;
					val_length=0;
				}
				
				if(buf[i] == '\n')
					status++;

				i++;
			}	
		}

		assert(count >= 0);
		assert(el_count == edges_count);
		assert(vl_count == vertices_count);

		free(main_buff_address);
		main_buff_address = NULL;
		buf = NULL;
		close(fd);
		fd = -1;

		g->offsets_list[g->vertices_count]=g->edges_count;

		printf("Reading %'.1f (MB) completed in %'.3f (seconds)\n", total_read_bytes/1e6, (get_nano_time() - t1)/1e9); 
	}

	print_ll_400_graph(g);

	return g;	
}

void __ll_400_webgraph_callback(paragrapher_read_request* req, paragrapher_edge_block* eb, void* in_offsets, void* in_edges, void* buffer_id, void* in_args)
{
	void** args = (void**) in_args;
	unsigned long* completed_callbacks_count = (unsigned long*)args[0];
	unsigned int* graph_edges = (unsigned int*)args[1];

	unsigned long* offsets = (unsigned long*)in_offsets;
	unsigned long ec = offsets[eb->end_vertex] + eb->end_edge - offsets[eb->start_vertex] - eb->start_edge;
	unsigned long dest_off = offsets[eb->start_vertex] + eb->start_edge;
	unsigned long* ul_in_edges = (unsigned long*)in_edges;

	// No need to parallelize this loop as multiple instances of the callbacks are concurrently called by the ParaGrapher 
	for(unsigned long e = 0; e < ec; e++, dest_off++)
		graph_edges[dest_off] = (unsigned int)ul_in_edges[e];

	paragrapher_csx_release_read_buffers(req, eb, buffer_id);

	__atomic_add_fetch(completed_callbacks_count, 1UL, __ATOMIC_RELAXED);

	return;
}

struct ll_400_graph* get_ll_400_webgraph(char* file_name, char* type)
{	
	// Opening graph
		unsigned long t1=get_nano_time();
			
		int ret = paragrapher_init();
		assert(ret == 0);

		paragrapher_graph_type pgt;
		// We should use PARAGRAPHER_CSX_WG_400_AP for PARAGRAPHER_CSX_WG_400_AP graphs but they can aslo be read by PARAGRAPHER_CSX_WG_800_AP.
		// To support all graphs with |V| <= 2 ^ 32, we read all of thme by PARAGRAPHER_CSX_WG_800_AP
		if(!strcmp(type, "PARAGRAPHER_CSX_WG_400_AP") || !strcmp(type, "PARAGRAPHER_CSX_WG_400_AP"))
			pgt = PARAGRAPHER_CSX_WG_800_AP;
		else
		{
			assert(0 && "get_ll_400_webgraph does not work for this type of graph.");
			return NULL;
		}
		paragrapher_graph* graph = paragrapher_open_graph(file_name, pgt, NULL, 0);
		assert(graph != NULL);

		unsigned long vertices_count = 0;
		unsigned long edges_count = 0;	
		{
			void* op_args []= {&vertices_count, &edges_count};

			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_GET_VERTICES_COUNT, op_args, 1);
			assert (ret == 0);
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_GET_EDGES_COUNT, op_args + 1, 1);
			assert (ret == 0);
			printf("Vertices: %'lu\n",vertices_count);
			printf("Edges: %'lu\n",edges_count);

			if(vertices_count >= (1UL << 32))
			{
				assert(0 && "get_ll_400_webgraph supports reading webgraphs with 4 Bytes ID per vertex.");
				return NULL;
			}

			// val = 1UL << (unsigned int)(log(edges_count)/log(2) - 3);
			// op_args[0] = &val;
			// ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_SET_BUFFER_SIZE, op_args, 1);
			// assert (ret == 0);
		}

	// Allocating memory
		struct ll_400_graph* g =calloc(sizeof(struct ll_400_graph),1);
		assert(g != NULL);
		g->vertices_count = vertices_count;
		g->edges_count = edges_count;
		g->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * (1 + g->vertices_count));
		assert(g->offsets_list != NULL);
		g->edges_list = numa_alloc_interleaved(sizeof(unsigned int) * g->edges_count);
		assert(g->edges_list != NULL);
		
	// Writing offsets
	{
		unsigned long* offsets = (unsigned long*)paragrapher_csx_get_offsets(graph, NULL, 0, -1UL, NULL, 0);
		assert(offsets != NULL);

		for(unsigned long v = 0; v <= vertices_count; v++)
			g->offsets_list[v] = offsets[v];

		paragrapher_csx_release_offsets_weights_arrays(graph, offsets);
		offsets = NULL;
	}

	// Reading edges
	{
		unsigned long completed_callbacks_count = 0;
		void* callback_args[] = {(void*)&completed_callbacks_count, (void*)g->edges_list};
		paragrapher_edge_block eb;
		eb.start_vertex = 0;
		eb.start_edge=0;
		eb.end_vertex = -1UL;
		eb.end_edge= -1UL;

		paragrapher_read_request* req= paragrapher_csx_get_subgraph(graph, &eb, NULL, NULL, __ll_400_webgraph_callback, (void*)callback_args, NULL, 0);
		assert(req != NULL);

		struct timespec ts = {0, 200 * 1000 * 1000};
		long status = 0;
		unsigned long read_edges = 0;
		unsigned long callbacks_count = 0;
		void* op0_args []= {req, &status};
		void* op1_args []= {req, &read_edges};
		void* op2_args []= {req, &callbacks_count};
		unsigned long next_edge_limit_print = 0;
		do
		{
			nanosleep(&ts, NULL);
			
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_STATUS, op0_args, 2);
			assert (ret == 0);
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_EDGES, op1_args, 2);
			assert (ret == 0);
			if(callbacks_count == 0)
			{
				ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_TOTAL_CALLBACKS, op2_args, 2);
				assert (ret == 0);
			}

			if(read_edges >= next_edge_limit_print)
			{
				printf("  Reading ..., status: %'ld, read_edges: %'lu, completed callbacks: %'u/%'u .\n", status, read_edges, completed_callbacks_count, callbacks_count);

				next_edge_limit_print += 0.05 * edges_count;
			}
		}
		while(status == 0);

		// printf("  Reading graph finished, status: %'ld, read_edges: %'lu, completed callbacks: %'u/%'u .\n", status, read_edges, completed_callbacks_count, callbacks_count);

		// Waiting for all buffers to be processed
		while(completed_callbacks_count < callbacks_count)
		{
			nanosleep(&ts, NULL);
			// printf("  Waiting for callbacks ..., completed callbacks: %'u/%'u .\n", completed_callbacks_count, callbacks_count);
		}
		// Releasing the req
		paragrapher_csx_release_read_request(req);
		req = NULL;
	}

	// Releasing the paragrapher graph
		ret = paragrapher_release_graph(graph, NULL, 0);
		assert(ret == 0);
		graph = NULL;
		
	printf("Reading completed in %'.3f (seconds)\n", (get_nano_time() - t1)/1e9); 

	print_ll_400_graph(g);

	return g;	
}


void __ll_404_webgraph_callback(paragrapher_read_request* req, paragrapher_edge_block* eb, void* in_offsets, void* in_edges, void* buffer_id, void* in_args)
{
	void** args = (void**) in_args;
	unsigned long* completed_callbacks_count = (unsigned long*)args[0];
	// Each edge has 4-Bytes for vertex ID and 4-Bytes for edge weight, we copy the 8-Bytes together
	unsigned long* graph_edges = (unsigned long*)args[1];

	unsigned long* offsets = (unsigned long*)in_offsets;
	unsigned long ec = offsets[eb->end_vertex] + eb->end_edge - offsets[eb->start_vertex] - eb->start_edge;
	unsigned long dest_off = offsets[eb->start_vertex] + eb->start_edge;
	unsigned long* ul_in_edges = (unsigned long*)in_edges;

	// No need to parallelize this loop as multiple instances of the callbacks are concurrently called by the ParaGrapher 
	for(unsigned long e = 0; e < ec; e++, dest_off++)
		graph_edges[dest_off] = ul_in_edges[e];

	paragrapher_csx_release_read_buffers(req, eb, buffer_id);

	__atomic_add_fetch(completed_callbacks_count, 1UL, __ATOMIC_RELAXED);

	return;
}

struct ll_404_graph* get_ll_404_webgraph(char* file_name, char* type)
{	
	// Opening graph
		unsigned long t1=get_nano_time();
			
		int ret = paragrapher_init();
		assert(ret == 0);

		paragrapher_graph_type pgt;
		if(!strcmp(type, "PARAGRAPHER_CSX_WG_404_AP"))
			pgt = PARAGRAPHER_CSX_WG_404_AP;
		else
		{
			assert(0 && "get_ll_404_webgraph does not work for this type of graph.");
			return NULL;
		}
		paragrapher_graph* graph = paragrapher_open_graph(file_name, pgt, NULL, 0);
		assert(graph != NULL);

		unsigned long vertices_count = 0;
		unsigned long edges_count = 0;	
		{
			void* op_args []= {&vertices_count, &edges_count};

			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_GET_VERTICES_COUNT, op_args, 1);
			assert (ret == 0);
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_GET_EDGES_COUNT, op_args + 1, 1);
			assert (ret == 0);
			printf("Vertices: %'lu\n",vertices_count);
			printf("Edges: %'lu\n",edges_count);

			// val = 1UL << (unsigned int)(log(edges_count)/log(2) - 3);
			// op_args[0] = &val;
			// ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_SET_BUFFER_SIZE, op_args, 1);
			// assert (ret == 0);
		}

	// Allocating memory
		struct ll_404_graph* g =calloc(sizeof(struct ll_404_graph),1);
		assert(g != NULL);
		g->vertices_count = vertices_count;
		g->edges_count = edges_count;
		g->offsets_list = numa_alloc_interleaved(sizeof(unsigned long) * (1 + g->vertices_count));
		assert(g->offsets_list != NULL);
		g->edges_list = numa_alloc_interleaved(2UL * sizeof(unsigned int) * g->edges_count);
		assert(g->edges_list != NULL);
		
	// Writing offsets
	{
		unsigned long* offsets = (unsigned long*)paragrapher_csx_get_offsets(graph, NULL, 0, -1UL, NULL, 0);
		assert(offsets != NULL);

		for(unsigned long v = 0; v <= vertices_count; v++)
			g->offsets_list[v] = offsets[v];

		paragrapher_csx_release_offsets_weights_arrays(graph, offsets);
		offsets = NULL;
	}

	// Reading edges
	{
		unsigned long completed_callbacks_count = 0;
		void* callback_args[] = {(void*)&completed_callbacks_count, (void*)g->edges_list};
		paragrapher_edge_block eb;
		eb.start_vertex = 0;
		eb.start_edge=0;
		eb.end_vertex = -1UL;
		eb.end_edge= -1UL;

		paragrapher_read_request* req= paragrapher_csx_get_subgraph(graph, &eb, NULL, NULL, __ll_404_webgraph_callback, (void*)callback_args, NULL, 0);
		assert(req != NULL);

		struct timespec ts = {0, 200 * 1000 * 1000};
		long status = 0;
		unsigned long read_edges = 0;
		unsigned long callbacks_count = 0;
		void* op0_args []= {req, &status};
		void* op1_args []= {req, &read_edges};
		void* op2_args []= {req, &callbacks_count};
		unsigned long next_edge_limit_print = 0;
		do
		{
			nanosleep(&ts, NULL);
			
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_STATUS, op0_args, 2);
			assert (ret == 0);
			ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_EDGES, op1_args, 2);
			assert (ret == 0);
			if(callbacks_count == 0)
			{
				ret = paragrapher_get_set_options(graph, PARAGRAPHER_REQUEST_READ_TOTAL_CALLBACKS, op2_args, 2);
				assert (ret == 0);
			}

			if(read_edges >= next_edge_limit_print)
			{
				printf("  Reading ..., status: %'ld, read_edges: %'lu, completed callbacks: %'u/%'u .\n", status, read_edges, completed_callbacks_count, callbacks_count);

				next_edge_limit_print += 0.05 * edges_count;
			}
		}
		while(status == 0);

		// printf("  Reading graph finished, status: %'ld, read_edges: %'lu, completed callbacks: %'u/%'u .\n", status, read_edges, completed_callbacks_count, callbacks_count);

		// Waiting for all buffers to be processed
		while(completed_callbacks_count < callbacks_count)
		{
			nanosleep(&ts, NULL);
			// printf("  Waiting for callbacks ..., completed callbacks: %'u/%'u .\n", completed_callbacks_count, callbacks_count);
		}
		// Releasing the req
		paragrapher_csx_release_read_request(req);
		req = NULL;
	}

	// Releasing the paragrapher graph
		ret = paragrapher_release_graph(graph, NULL, 0);
		assert(ret == 0);
		graph = NULL;
		
	printf("Reading completed in %'.3f (seconds)\n", (get_nano_time() - t1)/1e9); 

	print_ll_400_graph((struct ll_400_graph*)g);

	return g;	
}

void release_numa_interleaved_ll_400_graph(struct ll_400_graph* g)
{
	assert(g!= NULL && g->offsets_list != NULL);

	numa_free(g->offsets_list, sizeof(unsigned long)*(1 + g->vertices_count));
	g->offsets_list = NULL;

	if(g->edges_list)
	{
		numa_free(g->edges_list, sizeof(unsigned int) * g->edges_count);
		g->edges_list = NULL;
	}

	free(g);
	g = NULL;

	return;
}


void release_numa_interleaved_ll_800_graph(struct ll_800_graph* g)
{
	assert(g!= NULL && g->offsets_list != NULL);

	numa_free(g->offsets_list, sizeof(unsigned long)*(1 + g->vertices_count));
	g->offsets_list = NULL;

	if(g->edges_list)
	{
		numa_free(g->edges_list, sizeof(unsigned long) * g->edges_count);
		g->edges_list = NULL;
	}

	free(g);
	g = NULL;

	return;
}

void release_numa_interleaved_ll_404_graph(struct ll_404_graph* g)
{
	assert(g!= NULL && g->offsets_list != NULL);

	numa_free(g->offsets_list, sizeof(unsigned long)*(1 + g->vertices_count));
	g->offsets_list = NULL;

	if(g->edges_list)
	{
		numa_free(g->edges_list, 2UL * sizeof(unsigned int) * g->edges_count);
		g->edges_list = NULL;
	}

	free(g);
	g = NULL;

	return;
}

#endif