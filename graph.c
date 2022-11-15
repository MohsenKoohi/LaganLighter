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
#include <openssl/sha.h>
#include <fcntl.h>
#include <assert.h>
#include <mqueue.h>
#include <time.h>
#include <pthread.h>

#include "omp.c"

struct ll_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	unsigned int* edges_list;
};

// 4 bytes weight per edge
// each edge has two unsigned int elements in the edges_list, 
// the first one is the destination/source, and 
// the second one is the weight
struct w4_graph
{
	unsigned long vertices_count;
	unsigned long edges_count;
	unsigned long* offsets_list;
	unsigned int* edges_list;
};

void print_graph(struct ll_graph* ret)
{
	printf("\n|V|: %'20lu\n|E|: %'20lu\n", ret->vertices_count, ret->edges_count);
	printf("First offsets: ");
	for(unsigned int v=0; v<min(ret->vertices_count + 1, 20); v++)
		printf("%'lu, ", ret->offsets_list[v]);
	if(ret->vertices_count > 20)
	{
		printf("...\nLast offsets: ... ");
		for(unsigned int v = ret->vertices_count - 20; v <= ret->vertices_count; v++)
			printf(", %'lu", ret->offsets_list[v]);
	}

	if(ret->edges_list)
	{
		printf("\nFirst edges: ");
		for(unsigned long e=0; e<min(ret->edges_count, 20); e++)
			printf("%'u, ", ret->edges_list[e]);
		if(ret->edges_count > 20)
		{
			printf(" ...\nLast edges: ... ");
			for(unsigned long e = ret->edges_count - 20; e < ret->edges_count; e++)
				printf(", %'u", ret->edges_list[e]);
		}
	}

	printf("\n\n");

	return;
}

struct ll_graph* get_txt_graph(char* file_name)
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
		unsigned long uli_graph_size = sizeof(unsigned long) * ( 2 + vertices_count + 1) + sizeof(unsigned int) * edges_count;

	// Allocate memory
		struct ll_graph* g =calloc(sizeof(struct ll_graph),1);
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

		printf("Reading %'.1f (MB) completed in %'.1f (seconds)\n", total_read_bytes/1e6, (get_nano_time() - t1)/1e9); 
	}

	print_graph(g);

	return g;	
}


void release_numa_interleaved_graph(struct ll_graph* g)
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

#endif