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
		char* graph_name = NULL;
		{
			graph_name = strrchr(LL_INPUT_GRAPH_PATH, '/');
			if(graph_name == NULL)
				graph_name = LL_INPUT_GRAPH_PATH;
			else
				graph_name++;
			if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
				graph_name = strndup(graph_name, strrchr(graph_name, '.') - graph_name);
		}
		printf("\n");
		
	// Loading the graph
		int read_flags = 0;
		unsigned long load_time = - get_nano_time();
		struct ll_400_graph* csr_graph = NULL;
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"text"))
			// Reading the textual graph that does not require omp 
			csr_graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			csr_graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert(csr_graph != NULL);
		load_time += get_nano_time();

		unsigned long csr_vertices_count = csr_graph->vertices_count;
		unsigned long csr_edges_count = csr_graph->edges_count;

		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH, csr_graph->vertices_count,csr_graph->edges_count);
		
	// Initializing omp
		struct par_env* pe = initialize_omp_par_env();

	// Store graph in shm
		if(LL_STORE_INPUT_GRAPH_IN_SHM && (read_flags & 1U<<31) == 0)
			store_shm_ll_400_graph(pe, LL_INPUT_GRAPH_PATH, csr_graph, 0);
		if(read_flags & (1U << 31))
		{
			unsigned long mt = - get_nano_time();
			struct ll_400_graph* temp = copy_ll_400_graph(pe, csr_graph, NULL);
			mt += get_nano_time();
			printf("Graph copied in %'.3f seconds.\n", mt / 1e9);
			// release_shm_ll_400_graph(csr_graph);
			csr_graph = temp;
		}
	
	// Defining variables
		unsigned int* RA = NULL;
		struct ll_400_graph* csc_graph = NULL;
		
		unsigned long* exec_info = calloc(sizeof(unsigned long), 40 * 4);
		assert(exec_info != NULL);

		char* m2name [] = {"CSR", "CSR Rnd", "CSC", "CSC Rnd"};

		int max_m  = 1;

	for(unsigned int m = 0; m < max_m; m++)
	{
		// if(m == 1 ||  m == 3)
		// 	 continue;

		printf("\n\033[1;33mm = %u, %s\033[0;37m\n\n", m, m2name[m]);
		struct ll_400_graph* graph = NULL;

		// Setting the input graph
			if(m == 0)
				graph = csr_graph;
			else if(m == 1)
			{
				RA = get_create_fixed_random_ordering(pe, LL_INPUT_GRAPH_BASE_NAME, csr_graph->vertices_count, 5);
				graph = relabel_graph(pe, csr_graph, RA, 2); // 2: sort neighbour lists

				// csr_graph is not requried anymore
				// if(read_flags & 1U<<31)
				// 	release_shm_ll_400_graph(csr_graph);
				// else
					release_numa_interleaved_ll_400_graph(csr_graph);
				csr_graph = NULL;
			}
			else if(m == 2)
			{
				assert(csc_graph != NULL);
				graph = csc_graph;
			}
			else if(m == 3)
			{
				assert(RA != NULL);
				assert(csc_graph != NULL);

				graph = relabel_graph(pe, csc_graph, RA, 2); // 2: sort neighbour lists

				release_numa_interleaved_ll_400_graph(csc_graph);
				csc_graph = NULL;
			}

		// Calculating the transpose
			unsigned int flags = 0;
			if(graph->edges_count < 3e9)
				flags |= 3U; // 1U: Validation + 2U: Sort

			struct ll_400_graph* out_graph = potra(pe, graph, flags, exec_info + 40 * m);
			
		// Releasing mem
			if(m == 0)
			{
				csc_graph = out_graph;
			}
			else if(m == 1)
			{
				release_numa_interleaved_ll_400_graph(graph);
				graph = NULL;

				release_numa_interleaved_ll_400_graph(out_graph);
				out_graph = NULL;
			}
			else if(m == 2)
			{
				release_numa_interleaved_ll_400_graph(out_graph);
				out_graph = NULL;
			}
			else if(m == 3)
			{
				numa_free(RA, sizeof(unsigned int) * graph->vertices_count);
				RA = NULL;

				release_numa_interleaved_ll_400_graph(graph);
				graph = NULL;

				release_numa_interleaved_ll_400_graph(out_graph);
				out_graph = NULL;
			}
	}

	// Writing the report
	if(LL_OUTPUT_REPORT_PATH != NULL && access(LL_OUTPUT_REPORT_PATH, F_OK) == 0)
	{
		FILE* out = fopen(LL_OUTPUT_REPORT_PATH, "a");
		assert(out != NULL);

		if(LL_INPUT_GRAPH_BATCH_ORDER == 0)
		{
			fprintf(out, "%-20s; %-8s; %-8s; ", "Dataset", "|V|", "|E|");
			fprintf(out, "%-8s; %-10s; ", "m", "Time(s)");

			for(unsigned int e = 0; e < pe->hw_events_count; e++)
				fprintf(out, "%7s/kE; ", pe->hw_events_names[e]);

			fprintf(out, "%-8s; %-8s; %-8s; %-8s; %-8s; ","S1(s)", "S2(s)", "S3(s)", "Sort(s)", "Valid(s)");
			fprintf(out, "%-8s; %-8s; ","alpha", "k");
			fprintf(out, "%-8s; ","31LdImbl");
			fprintf(out, "%-8s; ","HDVEdgs%");
			fprintf(out, "%-8s; ","SplHVEg%");
			fprintf(out, "%-8s; ","#Byt/HDV");
			fprintf(out, "%-20s; ","Test Speedup (a/lh)");
			fprintf(out, "%-8s; ","ProcMeth");
			fprintf(out, "%-8s; ","Load (s)");
			fprintf(out, "%-10s; ","PkgEng(kJ)");
			fprintf(out, "%-10s; ","RAMEng(kJ)");
			fprintf(out, "%-10s; ","Avg Dif(M)");
			fprintf(out, "%-10s; ","AD/k|V|");
			fprintf(out, "\n");
		}

		char temp1 [16];
		char temp2 [16];			
		
		for(int m = 0; m < max_m; m++)
		{
			unsigned long* ei = exec_info + 40 * m;
			fprintf(out, "%-20s; %8s; %8s; ", graph_name, ul2s(csr_vertices_count, temp1), ul2s(csr_edges_count, temp2));
			if(ei[0] < 1e9)
				fprintf(out, "%-8s; %'10.3f; ", m2name[m], ei[0]/1e9);
			else
				fprintf(out, "%-8s; %'10.1f; ", m2name[m], ei[0]/1e9);

			for(unsigned int e = 0; e < pe->hw_events_count; e++)
			{
				double val = 1000.0 * ei[1 + e] / csr_edges_count;
				if(val < 1000)
					fprintf(out, "%10.1f; ", val);
				else
					fprintf(out, "%10s; ", ul2s(val, temp1));
			}
			for(int s = 10; s < 15; s++)
				if(ei[s] < 1e9)
					fprintf(out, "%'8.3f; ", ei[s]/1e9);
				else
					fprintf(out, "%'8.1f; ", ei[s]/1e9);

			fprintf(out, "%'8.3f; ", 1.0/ei[32]);
			fprintf(out, "%8s; ", ul2s(ei[33], temp1));
			fprintf(out, "%8lu; ", ei[34]);
			fprintf(out, "%'8.1f; ", 100.0*ei[35]/csr_edges_count);
			fprintf(out, "%'8.1f; ", 100.0*ei[22]/ei[20]);
			fprintf(out, "%8lu; ", ei[36]);
			fprintf(out, "%'20.2f; ", ei[37]/1e9);
			fprintf(out, "%8d; ", (int)ei[38]);
			fprintf(out, "%'8.1f; ", load_time / 1e9);
			fprintf(out, "%'10.1f; ", ei[18]/1e3);
			fprintf(out, "%'10.1f; ", ei[19]/1e3);
			fprintf(out, "%'10.1f; ", ei[28]/1e6);
			fprintf(out, "%'10.3f; ", 1000.0 * ei[28]/csr_vertices_count);
			
			fprintf(out, "\n");
		}

		fflush(out);
		fclose(out);
		out = NULL;
	}
	
	return 0;
}

