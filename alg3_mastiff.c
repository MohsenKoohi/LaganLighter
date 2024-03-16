#include "aux.c"
#include "graph.c"
#include "trans.c"
#include "msf.c"

/*
	
	MASTIFF: Structure-Aware Minimum Spanning Tree/Forest (MST/MSF)
		
	https://blogs.qub.ac.uk/DIPSA/MASTIFF-Structure-Aware-Minimum-Spanning-Tree-Forest/

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
		struct ll_400_graph* csr_graph = NULL;
		if(argc >= 3)
		{
			dataset = args[1];
			graph_type = args[2];
		}

		// get_ll_404_webgraph(dataset, graph_type);
		// exit(-1);

		if(!strcmp(graph_type,"text"))
			// Reading the textual graph that do not require omp 
			csr_graph = get_ll_400_txt_graph(dataset);
		if(!strncmp(graph_type,"PARAGRAPHER_CSX_WG_400",19))	
			// Reading a WebGraph using Poplar library
			csr_graph = get_ll_400_webgraph(dataset, graph_type);
		assert(csr_graph != NULL);

	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

		unsigned long* exec_info = calloc(sizeof(unsigned long), 20);
		assert(exec_info != NULL);

	// Symmetrizing and adding weights to the graph
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,csr_graph->vertices_count,csr_graph->edges_count);
		
		struct ll_400_graph* sym_graph = csr2sym(pe, csr_graph,  2U + 4U); // sort neighbour-lists and remove self-edges
		printf("SYM: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,sym_graph->vertices_count,sym_graph->edges_count);

		release_numa_interleaved_ll_400_graph(csr_graph);
		csr_graph = NULL;

		struct ll_404_graph* wgraph = add_4B_weight_to_ll_400_graph(pe, sym_graph, 1024*100, 0); // 1U: validate
		printf("Weighted: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",dataset,wgraph->vertices_count,wgraph->edges_count);

	// Running MSF
		
		// Mastiff
		struct msf* res_mastiff = NULL;
		
		res_mastiff = msf_mastiff(pe, wgraph, exec_info, 1U);

		assert(1 == msf_validate(pe, sym_graph ,res_mastiff, 0));
		
		struct msf* res_prim = NULL;
		if(wgraph->vertices_count < 1024)
		{
			// the implementation of prim changes the topology
			struct ll_404_graph* cwg = copy_ll_404_graph(pe, wgraph, NULL); 
			
			res_prim = msf_prim_serial(pe, cwg, 0);

			assert(1 == msf_validate(pe, sym_graph ,res_prim, 0));

			assert(res_mastiff->total_weight == res_prim->total_weight);
			printf("Total weight is \033[1;33m correct\033[0;37m.\n");
			
			release_numa_interleaved_ll_404_graph(cwg);
			cwg = NULL;
		}
			
		// Releasing graph and memory
			if(res_prim)
			{
				msf_free(res_prim);
				res_prim = NULL;
			}	
			if(res_mastiff)
			{
				msf_free(res_mastiff);
				res_mastiff = NULL;
			}

			release_numa_interleaved_ll_400_graph(sym_graph);
			sym_graph = NULL;

			release_numa_interleaved_ll_404_graph(wgraph);
			wgraph = NULL;


	printf("\n\n");
	
	return 0;
}
