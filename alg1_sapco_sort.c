#include "aux.c"
#include "graph.c"
#include "trans.c"
#include "relabel.c"

int main(int argc, char** args)
{	
	// Locale initialization
		setlocale(LC_NUMERIC, "");
		setbuf(stdout, NULL);
		setbuf(stderr, NULL);
		printf("\n");

	// Reading the grpah
		char* dataset = "data/test_csr.txt";
		char* graph_type = "text";
		struct ll_graph* graph = NULL;
		if(argc >= 3)
		{
			dataset = args[1];
			graph_type = args[2];
		}

		if(!strcmp(graph_type,"text"))
			// Reading the textual graph that do not require omp 
			graph = get_txt_graph(dataset);
		if(!strncmp(graph_type,"POPLAR_CSX_WG_",14))	
			// Reading a WebGraph using Poplar library
			graph = get_webgraph(dataset, graph_type);
	
		assert(graph != NULL);

	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

		unsigned long* exec_info = calloc(sizeof(unsigned long), 20);
		assert(exec_info != NULL);

	// Retrieving the graph
		struct ll_graph* csr_graph = graph;
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,csr_graph->vertices_count,csr_graph->edges_count);

		struct ll_graph* csc_graph = csr2csc(pe, csr_graph, 8U); // bit 3: do not write edges
		printf("CSC: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,csc_graph->vertices_count,csc_graph->edges_count);

	// Creating the reordering array
		unsigned int* RA_n2o = NULL;
		// for(unsigned int t=0; t<5;t++)
			RA_n2o = sapco_sort_degree_ordering(pe, csc_graph, exec_info, 1U); // bit 0: print

	// Checking the correctness
		assert(1 == relabeling_array_validate(pe, RA_n2o, graph->vertices_count));
		
		#pragma omp parallel for
		for(unsigned int v=1; v<csc_graph->vertices_count; v++)
		{
			unsigned int degree_v = csc_graph->offsets_list[RA_n2o[v] + 1] - csc_graph->offsets_list[RA_n2o[v]];
			unsigned int degree_v_1 = csc_graph->offsets_list[RA_n2o[v - 1] + 1] - csc_graph->offsets_list[RA_n2o[v - 1]];
			assert(degree_v_1 >= degree_v);
		}
		printf("Max-degree vertex, ID: %'u,   degree: %'u\n", RA_n2o[0], csc_graph->offsets_list[RA_n2o[0] + 1] - csc_graph->offsets_list[RA_n2o[0]]);
		
		printf("\n\t\t\033[1;34mArray is correct.\033[0;37m\n");

	// Releasing memory
		numa_free(RA_n2o, sizeof(unsigned int) * csc_graph->vertices_count);
		RA_n2o = NULL;

		release_numa_interleaved_graph(csc_graph);
		csc_graph = NULL;

		release_numa_interleaved_graph(csr_graph);
		csr_graph = NULL;
		graph = NULL;

	printf("\n\n");
	
	return 0;
}
