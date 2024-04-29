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
		printf("\n");

	// Reading the grpah
		char* dataset = "data/test_csr.txt";
		char* graph_type = "text";
		struct ll_400_graph* graph = NULL;
		if(argc >= 3)
		{
			dataset = args[1];
			graph_type = args[2];
		}

		if(!strcmp(graph_type,"text"))
			// Reading the textual graph that do not require omp 
			graph = get_ll_400_txt_graph(dataset);
		if(!strncmp(graph_type,"PARAGRAPHER_CSX_WG_400",19))	
			// Reading a WebGraph using ParaGrapher library
			graph = get_ll_400_webgraph(dataset, graph_type);
		assert(graph != NULL);

	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

		unsigned long* exec_info = calloc(sizeof(unsigned long), 20);
		assert(exec_info != NULL);

	// Retrieving the graph
		struct ll_400_graph* csr_graph = graph;
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,csr_graph->vertices_count,csr_graph->edges_count);
		
		struct ll_400_graph* sym_graph = csr2sym(pe, csr_graph,  2U + 4U); // sort neighbour-lists and remove self-edges
		printf("SYM: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,sym_graph->vertices_count,sym_graph->edges_count);

		release_numa_interleaved_ll_400_graph(csr_graph);
		csr_graph = NULL;
		graph = sym_graph;

	// CC
		unsigned int flags = 1U;  // 1U print stats
		unsigned int ccs_p = 0;
		unsigned int* cc_p = cc_pull(pe, graph, flags, exec_info, &ccs_p);
		unsigned int ccs_t = 0;
		unsigned int* cc_t = cc_thrifty_400(pe, graph, flags, &exec_info[10], &ccs_t);
		
	// Validating
		// (1) If two vertices are on the same componenet (i.e., they have the same cc_p), they should have the same cc_t
		#pragma omp parallel for
		for(unsigned v = 0; v < graph->vertices_count; v++)
			assert(cc_t[v] == cc_t[cc_p[v]]);
		
		/* (2) It is also required to check if two vertices are on different components (i.e., having the same cc_p), 
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

		cc_release(graph, cc_t);
		cc_t = NULL;
		cc_release(graph, cc_p);
		cc_p = NULL;

	
		release_numa_interleaved_ll_400_graph(graph);
		sym_graph = NULL;
		graph = NULL;

	printf("\n\n");
	
	return 0;
}
