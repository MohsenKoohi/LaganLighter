#include "aux.c"
#include "graph.c"
#include "trans.c"
#include "cc.c"

/*
	Thrifty Label Propagation Connected Components

	https://blogs.qub.ac.uk/graphprocessing/Thrifty-Label-Propagation-Fast-Connected-Components-for-Skewed-Degree-Graphs/

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
*/

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
			// Reading the textual graph that do not require omp 
			graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert(graph != NULL);
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n", LL_INPUT_GRAPH_PATH, graph->vertices_count, graph->edges_count);

	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

	// Store graph in shm
		if(LL_STORE_INPUT_GRAPH_IN_SHM && (read_flags & 1U<<31) == 0)
			store_shm_ll_400_graph(pe, LL_INPUT_GRAPH_PATH, graph, 0);
		
	// Exec info
		unsigned long* exec_info = calloc(sizeof(unsigned long), 32);
		assert(exec_info != NULL);

	// Symmetrizing the graph		
		if(!LL_INPUT_GRAPH_IS_SYMMETRIC)
		{
			struct ll_400_graph* sym_graph = symmetrize_graph(pe, graph,  2U + 4U); // sort neighbour-lists and remove self-edges

			// Releasing the input graph
				if(read_flags & 1U<<31)
					release_shm_ll_400_graph(graph);
				else
					release_numa_interleaved_ll_400_graph(graph);
				graph = sym_graph;
				sym_graph = NULL;
		}		
		printf("SYM: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH, graph->vertices_count, graph->edges_count);

	// CC
		unsigned int flags = 1U;  // 1U print stats
		unsigned int ccs_t = 0;
		unsigned int* cc_t = cc_thrifty_400(pe, graph, flags, &exec_info[10], &ccs_t);
		unsigned int max_degree_ID  = exec_info[10 + 9];
		
	// Validating
		if(0)
		{
			unsigned int ccs_p = 0;
			unsigned int* cc_p = cc_pull(pe, graph, flags, exec_info, &ccs_p);
		
			// (1) If two vertices are on the same componenet (i.e., they have the same cc_p), they should have the same cc_t
			#pragma omp parallel for
			for(unsigned v = 0; v < graph->vertices_count; v++)
			{
				assert(cc_t[v] == cc_t[cc_p[v]]);
				
				if(cc_t[v] == 0)
					assert(cc_p[v] == cc_p[max_degree_ID]);
				else
					assert(cc_p[v] == cc_p[cc_t[v] - 1]);
			}
			
			/* (2) It is also required to check if two vertices are on different components (i.e., not having the same cc_p), 
			they do not have the same cc_t. This is checked by matching the total number of compoents. 
			If two vertices are wrongly identified to have the same cc_t values, then either (i) we have 
			different number of compoenents, or (ii) we have the same number of components. Now, we prove that 
			(ii) cannot happen, i.e., (i) should be happened. If we have a correct number of components, and also
			two vertices on different components but with the same cc_t, then we have to have a component that is splitted 
			by cc_t, i.e., there are at least two vertices on the same component that their cc_t are different. This cannot happen 
			as it has been checked by (1). So, by matching the number of components, we can be sure that two vertices on differnt 
			components cannot be seen as connected by cc_t. 
			*/
			assert(ccs_p == ccs_t);

			printf("Validation:\t\t\033[1;33mCorrect\033[0;37m\n");
			cc_release(graph, cc_p);
			cc_p = NULL;
		}

	// Writing to the report
		if(LL_OUTPUT_REPORT_PATH != NULL)
		{
			FILE* out = fopen(LL_OUTPUT_REPORT_PATH, "a");
			if(out != NULL)
			{
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
					name, ul2s(graph->vertices_count, temp1), ul2s(graph->edges_count, temp2), exec_info[10 + 0] / 1e6);
				if(exec_info)
					for(unsigned int i=0; i<pe->hw_events_count; i++)					
						fprintf(out, " %'8s;", ul2s(exec_info[10 + i + 1], temp1));
				fprintf(out, "\n");

				if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
					free(name);
				name = NULL;

				fclose(out);
				out = NULL;
			}
		}

	// Releasing memory
		cc_release(graph, cc_t);
		cc_t = NULL;
		
		if(LL_INPUT_GRAPH_IS_SYMMETRIC)
		{
			if(read_flags & 1U<<31)
				release_shm_ll_400_graph(graph);
			else
				release_numa_interleaved_ll_400_graph(graph);
		}
		else
			release_numa_interleaved_ll_400_graph(graph);
		graph = NULL;

	printf("\n\n");
	
	return 0;
}
