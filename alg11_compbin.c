/*

Reading the graph, transforming (tranpsosing, symmetrizing, and randomizing), and writing in CompBin format

@misc{pg_fuse,
	title={Accelerating Loading WebGraphs in ParaGrapher}, 
	author={Mohsen {Koohi Esfahani}},
	year={2025},
	eprint={2507.00716},
	archivePrefix={arXiv},
	primaryClass={cs.DC},
  url={https://arxiv.org/abs/2507.00716}, 
}

WG2Compbin: https://github.com/MohsenKoohi/WG2CompBin

*/

#include "aux.c"
#include "graph.c"
#include "trans.c"

int main(int argc, char** args)
{	
	// Locale initialization
		setlocale(LC_NUMERIC, "");
		setbuf(stdout, NULL);
		setbuf(stderr, NULL);
		read_env_vars();
		printf("\n");

	// Requested transforms
		int transpose = 0;
		int symmetrize = 0;
		int randomize = 0;
		char* output_path = NULL;
		for(int r = 1; r < argc; r++)
		{
			if(!strcmp(args[r], "-t"))
				transpose = 1;

			if(!strcmp(args[r], "-s"))
				symmetrize = 1;

			if(!strcmp(args[r], "-r"))
				randomize = 1;

			if(!strcmp(args[r], "-o"))
			{
				assert(r + 1 < argc);
				output_path = strdup(args[r+1]);
			}
		}

		printf("Transform: transpose: %d, symmetrize: %d, randomize: %d\n", transpose, symmetrize, randomize);
		assert(output_path != NULL);
		printf("Output path: %s\n", output_path);
		assert(transpose == 0 || symmetrize == 0);

	// Reading the grpah
		struct ll_400_graph* graph = NULL;
		int read_flags = 0;
		
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"text"))
			// Reading the textual graph that do not require omp 
			graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert((read_flags & (1U<<31)) == 0);
		assert(graph != NULL);
		assert(graph->vertices_count >= (1UL<<24));
		assert(graph->vertices_count < (1UL<<32));
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n", LL_INPUT_GRAPH_PATH, graph->vertices_count, graph->edges_count);

	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

	// Transformations
		if(!LL_INPUT_GRAPH_IS_SYMMETRIC)
		{
			if(transpose)
			{
				struct ll_400_graph* csc_graph = atomic_transpose(pe, graph, 0); // 2: sort neighbors, 8: only offsets array
				assert(csc_graph != NULL);

				release_numa_interleaved_ll_400_graph(graph);
				graph = csc_graph;
				csc_graph = NULL;
			}

			if(symmetrize)
			{
				struct ll_400_graph* sym_graph = symmetrize_graph(pe, graph,  0); // 2U: sort neighbor-lists  4U: remove self-edges

				release_numa_interleaved_ll_400_graph(graph);
				graph = sym_graph;
				sym_graph = NULL;
		
				printf("SYM: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH, graph->vertices_count, graph->edges_count);
			}
		}

		if(randomize)
		{
			unsigned int* RA_o2n = get_create_fixed_random_ordering(pe, LL_INPUT_GRAPH_BASE_NAME, graph->vertices_count, 5);

			struct ll_400_graph* rel_graph = relabel_graph(pe, graph, RA_o2n, 0); // 2U: sort neighborlists
			assert(rel_graph != NULL);

			munmap(RA_o2n, sizeof(unsigned int) * graph->vertices_count);
			RA_o2n = NULL;
			release_numa_interleaved_ll_400_graph(graph);
			graph = rel_graph;
			rel_graph = NULL;	
		}

	// Writing the graph in CompBin format
		char* name = strrchr(LL_INPUT_GRAPH_PATH, '/');
		if(name == NULL)
			name = LL_INPUT_GRAPH_PATH;
		else
			name++;
		name = strdup(name);
		if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
			name = strndup(name, strrchr(name, '.') - name);

		char props_name[PATH_MAX] = {0};
		sprintf(props_name,"%s_props.txt", name);
		char offsets_name[PATH_MAX] = {0};
		sprintf(offsets_name,"%s_offsets.bin", name);
		char edges_name[PATH_MAX] = {0};
		sprintf(edges_name,"%s_edges.bin", name);

		// Writing the props.txt file
		{
			char path[PATH_MAX];
			sprintf(path, "%s/%s", output_path, props_name);
			FILE* f = fopen(path, "w");
			assert(f != NULL);
			fprintf(f, "vertices-count:%lu\n", graph->vertices_count);
			fprintf(f, "edges-count:%lu\n", graph->edges_count);
			fprintf(f, "bytes-per-vertex-ID-in-edges-file:4\n");
			fprintf(f, "offsets-file:%s\n", offsets_name);
			fprintf(f, "edges-file:%s\n", edges_name);
			fflush(f);
			fclose(f);
			f=NULL;

			printf("Writing props.txt file, %s,  completed.\n", path);
		}

		// Writing the offsets.bin file
		{
			char path[PATH_MAX];
			sprintf(path, "%s/%s", output_path, offsets_name);
			unsigned long offsets_file_size = sizeof(unsigned long) * (1 + graph->vertices_count);
			
			int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0644);
		 	if(fd == -1)
			{
				printf("\n\033[1;31mError Can't open file: %d, %s .\033[0;37m \n", errno, strerror(errno));
				exit(-1);
			}	

			int ret = ftruncate(fd, offsets_file_size);
			assert(ret == 0);

			unsigned long* offsets = (unsigned long*)mmap(
				NULL
				, offsets_file_size
				, PROT_READ | PROT_WRITE
				, MAP_SHARED 
				, fd
				, 0
			);
			if(offsets == MAP_FAILED)
			{
				printf("can't mmap : %d %s\n",errno, strerror(errno));
				exit(-1);
			}

			#pragma omp parallel for
			for(unsigned int v = 0; v < graph->vertices_count + 1; v++)
				offsets[v] = graph->offsets_list[v];

			ret = msync(offsets, offsets_file_size, MS_SYNC);
			assert(ret == 0);

			munmap(offsets, offsets_file_size);
			offsets = NULL;

			close(fd);
			fd = -1;

			printf("Writing offsets.bin file, %s, completed.\n", path);
		}

		// Writing the edges.bin file
		{
			char path[PATH_MAX];
			sprintf(path, "%s/%s", output_path, edges_name);
			unsigned long edges_file_size = sizeof(unsigned int) * graph->edges_count;
			
			int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0644);
		 	if(fd == -1)
			{
				printf("\n\033[1;31mError Can't open file: %d, %s .\033[0;37m \n", errno, strerror(errno));
				exit(-1);
			}	

			int ret = ftruncate(fd, edges_file_size);
			assert(ret == 0);

			unsigned int* edges = (unsigned int*)mmap(
				NULL
				, edges_file_size
				, PROT_READ | PROT_WRITE
				, MAP_SHARED 
				, fd
				, 0
			);
			if(edges == MAP_FAILED)
			{
				printf("can't mmap : %d %s\n",errno, strerror(errno));
				exit(-1);
			}

			#pragma omp parallel for
			for(unsigned long e = 0; e < graph->edges_count; e++)
				edges[e] = graph->edges_list[e];

			ret = msync(edges, edges_file_size, MS_SYNC);
			assert(ret == 0);

			munmap(edges, edges_file_size);
			edges = NULL;

			close(fd);
			fd = -1;

			printf("Writing edges.bin file, %s, completed.\n", path);
		}
		
	// Releasing memory
		release_numa_interleaved_ll_400_graph(graph);
		graph = NULL;

	printf("\n\n");
	
	return 0;
}
