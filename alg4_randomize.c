#include "aux.c"
#include "graph.c"
#include "trans.c"
#include "relabel.c"

struct ll_400_graph* graph = NULL;
FILE* plots = NULL;
struct par_env* pe = NULL;

unsigned int max_degree = 0;
unsigned int* degree_counters = NULL;
unsigned long* aid_sum = NULL;
unsigned int* edge_partitions = NULL;
struct dynamic_partitioning* dp = NULL;
unsigned int** thread_buffers = NULL;

unsigned int iterations = 5;

void plot_callback(unsigned int* RA, unsigned int iteration)
{
	if(iteration == -1U)
	{
		// Identifying max_degree
			#pragma omp parallel for reduction(max: max_degree)
			for(unsigned int v = 0; v < graph->vertices_count; v++)
			{
				unsigned long degree = graph->offsets_list[v+1] - graph->offsets_list[v];
				
				if(degree > max_degree)
					max_degree = degree;
			}

		// Memory allocation
			aid_sum = numa_alloc_interleaved(sizeof(unsigned long) * (1 + max_degree));
			assert(aid_sum != NULL);

			degree_counters = numa_alloc_interleaved(sizeof(unsigned int) * ( 1 + max_degree));
			assert(degree_counters != NULL);

		// Identifying degree_counters 
			#pragma omp parallel for 
			for(unsigned int v = 0; v < graph->vertices_count; v++)
			{
				unsigned long degree = graph->offsets_list[v+1] - graph->offsets_list[v];
				
				__atomic_add_fetch(degree_counters + degree, 1UL, __ATOMIC_RELAXED);

				aid_sum[degree] = 0;
			}


		// Allocating memory for thread_buffers
			thread_buffers = calloc(sizeof(unsigned int*), pe->threads_count);
			assert(thread_buffers != NULL);

			#pragma omp parallel 
			{
				unsigned tid = omp_get_thread_num();
				thread_buffers[tid] = numa_alloc_interleaved(sizeof(unsigned int) * (1 + max_degree));
				assert(thread_buffers != NULL);	
			}

		// Edge partitioning
			unsigned int thread_partitions = 64;
			unsigned int partitions_count = pe->threads_count * thread_partitions;
			edge_partitions = calloc(sizeof(unsigned int), partitions_count+1);
			assert(edge_partitions != NULL);
			dp = dynamic_partitioning_initialize(pe, partitions_count);
			parallel_edge_partitioning(graph, edge_partitions, partitions_count); 

		// Writing the x vals
			fprintf(plots,"\n<script type='text/javascript'>var data_%u_x = [", LL_INPUT_GRAPH_BATCH_ORDER);		
			for(unsigned int j = 2; j <= max_degree; j++)	
				if(degree_counters[j])
					fprintf(plots,"%d,",j);
			fprintf(plots,"];</script>");

			fflush(plots);
	}
	else
	{
		#pragma omp parallel for 
		for(unsigned int v = 0; v <= max_degree; v++)
			aid_sum[v] = 0;
	}

	// Iterating over edges of vertices
		#pragma omp parallel 
		{
			unsigned tid = omp_get_thread_num();

			unsigned int partition = -1U;
			while(1)
			{
				partition = dynamic_partitioning_get_next_partition(dp, tid, partition);
				if(partition == -1U)
					break; 

				for(unsigned int v = edge_partitions[partition]; v < edge_partitions[partition + 1]; v++)
				{
					unsigned int degree = (unsigned int)(graph->offsets_list[v + 1] - graph->offsets_list[v]);
					if(degree <= 1)
						continue;

					unsigned int c = 0;
					for(unsigned long e = graph->offsets_list[v]; e < graph->offsets_list[v + 1]; e++)
					{
						unsigned int neighbor = graph->edges_list[e];
						if(iteration == -1U)
							thread_buffers[tid][c] = neighbor;
						else
							thread_buffers[tid][c] = RA[neighbor];
						c++;
					}

					quick_sort_uint(thread_buffers[tid], 0, c - 1);

					unsigned long aid = 0;
					for(unsigned int e = 1; e < c; e++)
						aid += thread_buffers[tid][e] - thread_buffers[tid][e-1];

					aid /= degree;
					__atomic_add_fetch(aid_sum + degree, aid, __ATOMIC_RELAXED);
				}
			}
		}
		dynamic_partitioning_reset(dp);

	// Drawing the aid_sum
	{
		fprintf(plots,"\n<script type='text/javascript'>var data_%u_%u = {x: data_%u_x, y:[", 
			LL_INPUT_GRAPH_BATCH_ORDER, iteration, LL_INPUT_GRAPH_BATCH_ORDER);
		
		for(unsigned int j = 2; j <= max_degree ; j++)	
			if(degree_counters[j])
				fprintf(plots,"%.1f,", 1.0 * aid_sum[j] / degree_counters[j]);
		char name[32];
		if(iteration == -1U)
			sprintf(name, "Initial");
		else
			sprintf(name, "It %u", iteration);
		fprintf(plots,"], type: 'scatter',name: '%s', line: {shape: 'linear',dash: 'solid', width: 3}, mode: 'lines+markers', marker:{size:6}};</script>",  name);

		fflush(plots);	
	}

	return;
}

int main(int argc, char** args)
{	
	// Locale initialization
		setlocale(LC_NUMERIC, "");
		setbuf(stdout, NULL);
		setbuf(stderr, NULL);
		read_env_vars();
		printf("\n");

	// Making the plots file ready
	char plots_path [PATH_MAX];
	{
		if(LL_OUTPUT_REPORT_PATH == NULL)
			sprintf(plots_path, "logs/alg4_results.html");
		else
		{
			char* temp = strdup(LL_OUTPUT_REPORT_PATH);	
			char* dir_name = dirname(temp);
			sprintf(plots_path, "%s/results.html", dir_name);
			free(temp);
			temp = NULL;
			dir_name = NULL;
		}
		printf("Plots file path: %s\n", plots_path);
		
		if(access(plots_path, F_OK) != 0 || LL_INPUT_GRAPH_BATCH_ORDER == 0)
		{
			plots = fopen(plots_path, "w");
			assert(plots != NULL);

			fprintf(plots, "<html>\n<head><title>N2N AID</title></head>\n"
				"<body style='margin:0;font-family:tahoma'>\n"
					"<script src='https://hpgp.net/LaganLighter/scripts/jquery-1.11.1.min.js'></script>\n"
					"<script src='https://hpgp.net/LaganLighter/scripts/plotly.js'></script>\n"
					"<script src='https://hpgp.net/LaganLighter/scripts/scripts.js'></script>\n"
					"<link rel='stylesheet' type='text/css' href='https://hpgp.net/LaganLighter/styles/skeleton.css'/>\n"
					"<div class='container'>\n"
			);
		}
		else
		{
			plots = fopen(plots_path, "a");
			assert(plots != NULL);
		}

		fprintf(plots, "\n<div class='row' style='margin-bottom:20px'>");
		fprintf(plots,"\n<div class='div-chart twelve columns' style='border:1px solid gray;padding:2px'><div id='Div%d' style='height:800px;'></div></div>", LL_INPUT_GRAPH_BATCH_ORDER);

		fflush(plots);
	}

	// Reading the grpah
		int read_flags = 0;
		unsigned long load_time = - get_nano_time();
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"text"))
			// Reading the textual graph that does not require omp 
			graph = get_ll_400_txt_graph(LL_INPUT_GRAPH_PATH, &read_flags);
		if(!strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_400_AP") || !strcmp(LL_INPUT_GRAPH_TYPE,"PARAGRAPHER_CSX_WG_800_AP"))
			// Reading a WebGraph using ParaGrapher library
			graph = get_ll_400_webgraph(LL_INPUT_GRAPH_PATH, LL_INPUT_GRAPH_TYPE, &read_flags);
		assert(graph != NULL);
		load_time += get_nano_time();
		
	// Initializing omp
		pe = initialize_omp_par_env();

	// Store graph in shm
		if(LL_STORE_INPUT_GRAPH_IN_SHM && (read_flags & 1U<<31) == 0)
			store_shm_ll_400_graph(pe, LL_INPUT_GRAPH_PATH, graph, 0);
	
		printf("CSR: %-30s;\t |V|: %'20lu;\t |E|:%'20lu;\n",LL_INPUT_GRAPH_PATH,graph->vertices_count,graph->edges_count);


	// Randomization
		plot_callback(NULL, -1U);
		unsigned long process_time = - get_nano_time();
		unsigned int* RA_o2n = random_ordering(pe, graph->vertices_count, iterations, plot_callback);
		process_time += get_nano_time();
		assert(1 == relabeling_array_validate(pe, RA_o2n, graph->vertices_count));

	// Closing the div in the plots file
	{
		int d = LL_INPUT_GRAPH_BATCH_ORDER;
		fprintf(plots, "\n<script type='text/javascript'> var data%d = [", d);
		fprintf(plots, "data_%u_%u, ", d, -1);
		for(int it = 0; it < iterations; it++)
			fprintf(plots, "data_%u_%u,", d, it);

		char* temp = strdup(LL_INPUT_GRAPH_PATH);	
		char* name = strrchr(LL_INPUT_GRAPH_PATH, '/');
		if(name == NULL)
			name = LL_INPUT_GRAPH_PATH;
		else
			name++;
		if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
			name = strndup(name, strrchr(name, '.') - name);
		
		fprintf(plots,
			"];\n"
				"var layout%d = {"
					"title: { text:'N2N AID, %s'	},"
				  	"xaxis: {title:'Degree',gridcolor:'#888',linecolor: '#000',type: 'log', autorange: true,ticks: 'outside'},"
				    "yaxis: {gridcolor:'#888',linecolor: '#000',ticks: 'outside'},"
				    "margin: {r:50,l:100,b:50,t:50},"
				    "legend: {orientation: 'h'},"
				    "font: { family: 'Times New Roman',size: 24, color:'#000'}"
				"};\n"
				"Plotly.newPlot('Div%d', data%d, layout%d);\n"
			"</script></div><br><br>\n\n", d, name, d,d,d
		);
		fflush(plots);

		fclose(plots);
		plots = NULL;
	}

	// Writing to the report
	if(LL_OUTPUT_REPORT_PATH != NULL && access(LL_OUTPUT_REPORT_PATH, F_OK) == 0)
	{
		FILE* out = fopen(LL_OUTPUT_REPORT_PATH, "a");
		assert(out != NULL);

		if(LL_INPUT_GRAPH_BATCH_ORDER == 0)
		{
			fprintf(out, "%-20s; %-8s; %-8s; %-10s; %-15s; %-15s;", "Dataset", "|V|", "|E|", "Max. Deg.", "Load Time (ms)", "Pr Time (ms)");
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
		
		fprintf(out, "%-20s; %8s; %8s; %'10u; %'15.1f; %'15.1f;", 
			name, ul2s(graph->vertices_count, temp1), ul2s(graph->edges_count, temp2), max_degree, load_time / 1e6, process_time/1e6);
		fprintf(out, "\n");

		if(strcmp(LL_INPUT_GRAPH_TYPE,"text") == 0)
			free(name);
		name = NULL;

		fclose(out);
		out = NULL;
	}

	printf("\n\n");
	
	return 0;
}
