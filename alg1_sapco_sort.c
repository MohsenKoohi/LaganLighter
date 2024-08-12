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
		read_env_vars();
		printf("\n");

	// Reading the grpah
		struct ll_400_graph* graph = NULL;
		int read_flags = 0;
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"text"))
			// Reading the textual graph that does not require omp 
			graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert(graph != NULL);
		
	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

	// Store graph in shm
		if(LL_STORE_INPUT_GRAPH_IN_SHM & (read_flags & 1U<<31) == 0)
			store_shm_ll_400_graph(pe, LL_INPUT_GRAPH_PATH, graph);
	
	// Exec info	
		unsigned long* exec_info = calloc(sizeof(unsigned long), 20);
		assert(exec_info != NULL);

	// Transposing the input CSR graph
		struct ll_400_graph* csr_graph = graph;
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,csr_graph->vertices_count,csr_graph->edges_count);

		struct ll_400_graph* csc_graph = csr2csc(pe, csr_graph, 8U); // bit 3: do not write edges
		printf("CSC: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,csc_graph->vertices_count,csc_graph->edges_count);

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

	// Writing to the report
		if(LL_OUTPUT_REPORT_PATH != NULL)
		{
			FILE* out = fopen(LL_OUTPUT_REPORT_PATH, "a");
			assert(out != NULL);

			if(LL_INPUT_GRAPH_BATCH_ORDER == 0)
			{
				fprintf(out, "%-20s; %-8s; %-8s; %-13s;", "Dataset", "|V|", "|E|", "Time (ms)");
				if(exec_info)
					for(unsigned int i=0; i<pe->hw_events_count; i++)
						fprintf(out, " %-8s;", pe->hw_events_names[i]);
				fprintf(out, "\n");
			}

			char temp1 [16];
			char temp2 [16];			
			char* name = strrchr(LL_INPUT_GRAPH_PATH, '/');
			if(name == NULL)
				name = LL_INPUT_GRAPH_PATH;
			else
				name++;
			if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
				name = strndup(name, strrchr(name, '.') - name);
			
			fprintf(out, "%-20s; %'8s; %'8s; %'13.1f;", 
				name, ul2s(csc_graph->vertices_count, temp1), ul2s(csc_graph->edges_count, temp2), exec_info[0] / 1e6);
			if(exec_info)
				for(unsigned int i=0; i<pe->hw_events_count; i++)					
					fprintf(out, " %'8s;", ul2s(exec_info[i + 1], temp1));
			fprintf(out, "\n");

			if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
				free(name);
			name = NULL;

			fclose(out);
			out = NULL;
		}

	// Releasing memory
		numa_free(RA_n2o, sizeof(unsigned int) * csc_graph->vertices_count);
		RA_n2o = NULL;

		release_numa_interleaved_ll_400_graph(csc_graph);
		csc_graph = NULL;

		if(read_flags & 1U<<31)
			release_shm_ll_400_graph(csr_graph);
		else
			release_numa_interleaved_ll_400_graph(csr_graph);
		csr_graph = NULL;
		graph = NULL;

	printf("\n\n");
	
	return 0;
}
