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
		read_env_vars();
		printf("\n");

	// Reading the grpah
		struct ll_400_graph* csr_graph = NULL;
		struct ll_400_graph* sym_graph = NULL;
		struct ll_404_graph* wgraph = NULL;
		int read_flags = 0;
		
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"text"))
			// Reading the textual graph that do not require omp 
			csr_graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			csr_graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_404_AP"))	
			wgraph = get_ll_404_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert(csr_graph != NULL || wgraph != NULL);
		
	// Initializing omp
		struct par_env* pe= initialize_omp_par_env();

	// Store input graph in shm
		if(LL_STORE_INPUT_GRAPH_IN_SHM && (read_flags & 1U<<31) == 0)
		{
			if(csr_graph != NULL)
				store_shm_ll_400_graph(pe, LL_INPUT_GRAPH_PATH, csr_graph);
			else
				store_shm_ll_404_graph(pe, LL_INPUT_GRAPH_PATH, wgraph);
		}
		
	// Initializing exec info
		unsigned long* exec_info = calloc(sizeof(unsigned long), 20);
		assert(exec_info != NULL);

	// Symmetrizing and adding weights to the graph if it is not weighted
		if(wgraph == NULL)
		{
			printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,csr_graph->vertices_count,csr_graph->edges_count);
			
			if(LL_INPUT_GRAPH_IS_SYMMETRIC == 0)
			{
				sym_graph = csr2sym(pe, csr_graph,  2U + 4U); // sort neighbour-lists and remove self-edges
				printf("SYM: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,sym_graph->vertices_count,sym_graph->edges_count);

				if(read_flags & 1U<<31)
					release_shm_ll_400_graph(csr_graph);
				else
					release_numa_interleaved_ll_400_graph(csr_graph);
			}
			else
				sym_graph = csr_graph;

			csr_graph = NULL;

			wgraph = add_4B_weight_to_ll_400_graph(pe, sym_graph, 1024*100, 0); // 1U: validate
			printf("Weighted: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,wgraph->vertices_count,wgraph->edges_count);
		}
		else
		{
			if(LL_INPUT_GRAPH_IS_SYMMETRIC == 0)
			{
				printf("To do: symmetrizing weighted graphs ... \n");
				exit(-1);
			}

			// Is needed for validation, `msf_validate()`. Can be skipped.
			#ifndef NDEBUG 
				sym_graph = copy_ll_404_to_400_graph(pe, wgraph, NULL);
				assert(sym_graph != NULL);
			#endif
		}

	// Running MSF
		// MASTIFF
		struct msf* res_mastiff = NULL;
		
		res_mastiff = msf_mastiff(pe, wgraph, exec_info, 1U);

		assert(1 == msf_validate(pe, sym_graph, res_mastiff, 0));
		
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
				name, ul2s(wgraph->vertices_count, temp1), ul2s(wgraph->edges_count, temp2), exec_info[0] / 1e6);
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

		if(sym_graph != NULL)
		{
			if(
				(strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") == 0 || strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP") == 0) 
				&& LL_INPUT_GRAPH_IS_SYMMETRIC && (read_flags & 1U<<31) == 1
			)
				release_shm_ll_400_graph(sym_graph);
			else
				release_numa_interleaved_ll_400_graph(sym_graph);
			sym_graph = NULL;
		}

		if(strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_404_AP") == 0 && (read_flags & 1U<<31) == 1)
			release_shm_ll_404_graph(wgraph);
		else
			release_numa_interleaved_ll_404_graph(wgraph);
			
		wgraph = NULL;


	printf("\n\n");
	
	return 0;
}
