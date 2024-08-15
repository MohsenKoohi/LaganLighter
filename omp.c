#ifndef __OMP_C
#define __OMP_C 1

#include <omp.h>
#include <locale.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stddef.h>
#include <numa.h>
#include <unistd.h>
#include <numaif.h>
#include <cpuid.h>
#include <papi.h>
#include <sched.h>

unsigned int papi_events []= {
	PAPI_LST_INS,
	PAPI_L3_TCM,
	// PAPI_L2_TCM,
	PAPI_TOT_INS,
	PAPI_RES_STL,
	PAPI_TLB_DM,
	// PAPI_TLB_IM,
	PAPI_BR_MSP,
	// PAPI_BR_INS,
	PAPI_TOT_CYC
};
 
// Print Time and Idle Percentage
#define PTIP(step_name) \
  printf("%-60s\t\t %'10.2f (ms) \t(%5.2f%%)\n",step_name, mt/1e6, get_idle_percentage(mt, ttimes, omp_get_num_threads()));

#define PT(step_name) \
	printf("%-60s\t\t %'10.2f (ms)\n",step_name, mt/1e6); 

double get_idle_percentage(unsigned long nt, unsigned long* threads_nt, unsigned int threads_count)
{ 
	unsigned long idle = 0; 
	for(unsigned int t=0; t<threads_count; t++)
		idle += nt - threads_nt[t]; 
	idle /= threads_count; 
	return 100.0 * idle / nt;
} 

unsigned long omp_get_thread_num_ulong()
{
	return (unsigned long)omp_get_thread_num();
}

void papi_init()
{
	int ret = PAPI_library_init(PAPI_VER_CURRENT);
	assert(ret == PAPI_VER_CURRENT);

	ret=PAPI_thread_init(omp_get_thread_num_ulong);
	assert(ret == PAPI_OK);

	return;
}

unsigned long papi_start(unsigned int* in_events, unsigned int in_events_count)
{
	assert(in_events!= NULL && in_events_count != 0);

	unsigned int event_set = PAPI_NULL;
	int ret = PAPI_create_eventset(&event_set);
	assert(ret == PAPI_OK);

	unsigned long events_count = 0;
	for(unsigned int i=0; i<in_events_count; i++)
	{
		ret = PAPI_add_event(event_set, in_events[i]);
		if(ret != PAPI_OK)
		{
			if(omp_get_thread_num() == 0)
			{
				char event_name[256]="";
				PAPI_event_code_to_name(in_events[i], event_name);
				printf("PAPI error for index %u, event %s (%x), %d: %s\n", i, event_name, in_events[i], ret, PAPI_strerror(ret));
			}
		}
		else
		{
			if(omp_get_thread_num() == 0)
			{
				char event_name[256]="";
				PAPI_event_code_to_name(in_events[i], event_name);
				printf("PAPI index: %u, events_count %lu, %s (%x), added.\n", i, events_count, event_name, in_events[i]);
			}
			events_count++;
		}
	}

	if(events_count == 0)
		return 0UL;

	ret = PAPI_start(event_set);
	if(ret != PAPI_OK)
	{
		printf("PAPI can't start, %d: %s\n", ret, PAPI_strerror(ret));
		// exit(-1);
		return 0UL;
	}
	
	return (events_count << 32) + event_set;
}

void papi_reset(unsigned long papi_arg)
{
	unsigned long events_count = (papi_arg >> 32);
	if (events_count == 0)
		return;

	unsigned int event_set = (unsigned int)papi_arg;
	int ret = PAPI_reset(event_set);
	assert( ret == PAPI_OK );
	
	return;
} 

void papi_stop(unsigned long papi_arg)
{
	unsigned long events_count = (papi_arg >> 32);
	if (events_count == 0)
		return;

	unsigned int event_set = (unsigned int)papi_arg;
	unsigned long long temp_values[32];
	assert(events_count <= 32);
	
	int ret = PAPI_stop(event_set, temp_values);
	if(ret != PAPI_OK)
		printf("PAPI can't stop, %d: %s\n", ret, PAPI_strerror(ret));

	return;
}

int papi_read(unsigned long papi_arg, unsigned long* in_values)
{
	assert(in_values != NULL);
	unsigned int event_set = (unsigned int) papi_arg;
	unsigned long events_count = (papi_arg >> 32);
	if(events_count == 0)
		return -1;

	unsigned long long temp_values[32];
	assert(events_count <= 32);
	int ret = PAPI_read(event_set, temp_values);
	if(ret != PAPI_OK)
	{
		printf("PAPI can't read, %d: %s\n", ret, PAPI_strerror(ret));
		return -1;
	}

	for(unsigned int i=0; i<events_count; i++)
		in_values[i] = temp_values[i];

	return 0;
}

struct par_env
{
	char hostname[128];
	char cpu_brand[16];
	unsigned int cpuid_max_eax;
	unsigned int cpu_family;
	unsigned int cpu_model;

	unsigned int L1_coherency_line_size;
	unsigned int L1_number_of_sets;
	unsigned int L1_ways_of_associativity;
	unsigned int L1_count;
	unsigned int L1_cache_size;
	unsigned long L1_caches_total_size;

	unsigned int L2_coherency_line_size;
	unsigned int L2_number_of_sets;
	unsigned int L2_ways_of_associativity;
	unsigned int L2_count;
	unsigned int L2_cache_size;
	unsigned long L2_caches_total_size;

	unsigned int L3_coherency_line_size;
	unsigned int L3_number_of_sets;
	unsigned int L3_ways_of_associativity;
	unsigned int L3_count;
	unsigned int L3_cache_size;
	unsigned long L3_caches_total_size;

	unsigned int nodes_count;
	unsigned int cpus_count;

	//converting cpu_id to node_id
	unsigned int * cpu2node;

	// list of cpus for each node
	unsigned int ** node_cpus;
	unsigned int * node_cpus_length;

	// Sibling groups
	unsigned int sibling_groups_count;
	unsigned int * node_sibling_groups_start_ID;  // A node ID is used as index to return the ID of the first sibling group of the node
	unsigned int * sibling_group_cpus_start_offsets;  // An ID of a sibling group is used as index to return the start index in `sibling_groups_cpus`  
	unsigned int * sibling_groups_cpus;

	// threads on each node
	unsigned int ** node_threads;
	unsigned int * node_threads_length;

	// node and cpu of each thread
	unsigned int threads_count;
	unsigned int* thread2node;
	unsigned int* thread2cpu;
	unsigned int** threads_next_threads;

	// papi args
	unsigned long* papi_args;
	unsigned int hw_events_count;
	unsigned long hw_events [32];
	char hw_events_names[32][PAPI_MAX_STR_LEN];
};

int thread_papi_read(struct par_env* pe)
{
	unsigned long temp_vals[32]={0};
	unsigned long arg = pe->papi_args[omp_get_thread_num()];
	// if(arg == 0)
	// 	return -2;

	if(arg != 0)
	{
		int ret = papi_read(arg, temp_vals);
		// if(ret != 0)
		// 	return -1;
		
		if(ret == 0)
			for(unsigned int e=0; e<sizeof(papi_events)/sizeof(papi_events[0]); e++)
				__atomic_add_fetch(&pe->hw_events[e], temp_vals[e], __ATOMIC_SEQ_CST);
	}
	
	return 0;
}

void print_hw_events(struct par_env* pe, unsigned int iterations)
{
	if(iterations == 0)
		iterations = 1;

	printf("\nHW Events:\n");
	for(unsigned int e = 0; e < pe->hw_events_count; e++)
		printf("\t%-20s: %'20lu\n", pe->hw_events_names[e], pe->hw_events[e] / iterations);

	return;
}

void reset_hw_events(struct par_env* pe)
{
	for(int e = 0; e < pe->hw_events_count; e++)
		pe->hw_events[e] = 0;
	
	return;
}

void copy_reset_hw_events(struct par_env* pe, unsigned long* ev, unsigned int iterations)
{
	if(ev)
	{
		if(iterations == 0)
			iterations = 1;

		for(unsigned int e = 0; e < pe->hw_events_count; e++)
			ev[e] = pe->hw_events[e] / iterations;
	}

	reset_hw_events(pe);

	return;
}

struct par_env* initialize_omp_par_env()
{
	// NUMA Initialization
		assert(numa_available() != -1 && "Can't initialize numa");
		numa_set_strict(1);

		struct par_env* pe= calloc(1,sizeof(struct par_env));
		assert(pe != NULL);

	// hostname
	{
		gethostname(pe->hostname, 127);
		printf("\n\nHost name: \033[1;32m%s\033[0;37m\n",pe->hostname);
	}

	// CPU info
		{
			unsigned int* ebx = (unsigned int*)&pe->cpu_brand[0];
			unsigned int* ecx = (unsigned int*)&pe->cpu_brand[8];
			unsigned int* edx = (unsigned int*)&pe->cpu_brand[4];
			pe->cpu_brand[13]=0;
			__cpuid_count(0, 0, pe->cpuid_max_eax, *ebx, *ecx, *edx);
			printf("CPU Manufacturer: \033[1;34m%s\033[0;37m (Max EAX: %u)\n",pe->cpu_brand,pe->cpuid_max_eax);
		}

		{
			unsigned int eax, ebx, ecx, edx;
			__cpuid_count(1, 0, eax, ebx, ecx, edx);
			pe->cpu_family = ((eax & 0xf00)>>8) + ((eax & 0xff00000)>>20);
			printf("CPU Family: %u\n", pe->cpu_family);
	
			pe->cpu_model = ((eax & 0xf0)>>4) + ((eax & 0xf0000)>>12);
			// if(((eax & 0xf00)>>8) < 15)
			// 	pe->cpu_model = (eax & 0xf0)>>4;
			printf("CPU Model: %u\n", pe->cpu_model);

			#ifdef __GNUC__
				printf("GCC Version: %u.%u.%u\n", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
			#endif
		}

	// Reading cache info
		printf("\n");
		for(int i=0; i<=3; i++)
		{
			char file_name[255];
			int count;

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/type",i);
			char cache_type[255];
			count = get_file_contents(file_name, cache_type, 255);
			assert(count > 0);
			cache_type[count-1]=0;   				// removing \n at the end of file

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/size",i);
			char cache_size_char[255];
			count = get_file_contents(file_name, cache_size_char, 255);
			assert(count > 0);
			cache_size_char[count-1]=0;  			// removing \n at the end of file
			unsigned int cache_size = atol(cache_size_char);
			if(cache_size_char[count - 2] == 'K')
				cache_size *= 1024;

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/level",i);
			char cache_level_char[255];
			count = get_file_contents(file_name, cache_level_char, 255);
			assert(count > 0);
			cache_level_char[count-1]=0;  			// removing \n at the end of file
			unsigned int cache_level = atol(cache_level_char);

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/coherency_line_size",i);
			char coherency_line_size_char[255];
			count = get_file_contents(file_name, coherency_line_size_char, 255);
			assert(count > 0);
			coherency_line_size_char[count-1]=0;  			// removing \n at the end of file
			unsigned int coherency_line_size = atol(coherency_line_size_char);

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/number_of_sets",i);
			char number_of_sets_char[255];
			count = get_file_contents(file_name, number_of_sets_char, 255);
			assert(count > 0);
			number_of_sets_char[count-1]=0;  			// removing \n at the end of file
			unsigned int number_of_sets = atol(number_of_sets_char);

			sprintf(file_name, "/sys/devices/system/cpu/cpu0/cache/index%d/ways_of_associativity",i);
			char ways_of_associativity_char[255];
			count = get_file_contents(file_name, ways_of_associativity_char, 255);
			assert(count > 0);
			ways_of_associativity_char[count-1]=0;  			// removing \n at the end of file
			unsigned int ways_of_associativity = atol(ways_of_associativity_char);

			unsigned int caches_count;
			{
				char temp[128];
				sprintf(file_name, "cat /sys/devices/system/cpu/cpu*/cache/index%d/shared_cpu_map|grep -v \"^$\"|sort|uniq| wc -l", i);
				long ret = run_command(file_name, temp, 128);
				assert((int)ret == 0);
				caches_count = atoi(temp);
			}

			if(strcmp(cache_type,"Instruction"))
				if(cache_level == 1)
				{
					pe->L1_cache_size = cache_size;
					pe->L1_coherency_line_size = coherency_line_size;
					pe->L1_number_of_sets = number_of_sets;
					pe->L1_ways_of_associativity = ways_of_associativity;
					pe->L1_count = caches_count;
					pe->L1_caches_total_size = 1UL * pe->L1_count * pe->L1_cache_size;
				}
				else if(cache_level == 2)
				{
					pe->L2_cache_size = cache_size;
					pe->L2_coherency_line_size = coherency_line_size;
					pe->L2_number_of_sets = number_of_sets;
					pe->L2_ways_of_associativity = ways_of_associativity;
					pe->L2_count = caches_count;
					pe->L2_caches_total_size = 1UL * pe->L2_count * pe->L2_cache_size;
				}
				else if(cache_level == 3)
				{
					pe->L3_cache_size = cache_size;
					pe->L3_coherency_line_size = coherency_line_size;
					pe->L3_number_of_sets = number_of_sets;
					pe->L3_ways_of_associativity = ways_of_associativity;
					pe->L3_count = caches_count;
					pe->L3_caches_total_size = 1UL * pe->L3_count * pe->L3_cache_size;
				}
			
			printf("Cache Level: \033[3;31m%u\033[0;37m\n"
				"  Type: \033[3;33m%-20s\033[0;37m; Size: \033[3;36m%'20u\033[0;37m (%s);\n"
				"  Line Length: \033[3;35m%'7u\033[0;37m Bytes; Number of sets: %'16u; Associate Ways: %5u;\n"
				"  Count: %19u; Total size: %15.2f (MB);\n", 
				cache_level, cache_type, cache_size, cache_size_char, 
				coherency_line_size, number_of_sets, ways_of_associativity, 
				caches_count, caches_count *  cache_size / 1024.0 / 1024
			);
		}

	// Mem info
		pe->nodes_count = numa_num_task_nodes();
		printf("\nNUMA nodes: \033[1;31m%d\033[0;37m\n", pe->nodes_count);

		for(int i=0; i<pe->nodes_count; i++)
		{
			unsigned long free_mem = 0;
			unsigned long mem=numa_node_size(i, &free_mem);
			mem/=(1024*1024*1024);
			free_mem/=(1024*1024*1024);

			printf("Node \033[1;32m%d\033[0;37m memory : \033[1;33m%lu\033[0;37m GB, free: %lu GB\n", i, mem, free_mem);
		}
		printf("\n");

	// CPUs
		pe->cpus_count = numa_num_configured_cpus();
		printf("# CPUS: %'u\n",pe->cpus_count);
		pe->cpu2node=calloc(pe->cpus_count, sizeof(unsigned int));
		struct bitmask *bm = numa_allocate_cpumask();

		pe->node_cpus=calloc(pe->nodes_count , sizeof(unsigned int*));
		pe->node_cpus_length=calloc(pe->nodes_count , sizeof(unsigned int));
		assert(pe->cpu2node != NULL && pe->node_cpus != NULL && pe->node_cpus_length != NULL);

		for(int i=0; i< pe->nodes_count; i++)
		{
			pe->node_cpus[i]=calloc(pe->cpus_count, sizeof(unsigned int));
			assert(pe->node_cpus[i] != NULL);

			numa_bitmask_clearall(bm);
			assert(0 == numa_node_to_cpus(i,bm));

			printf("CPUs on node \033[1;35m%d\033[0;37m : ",i);
			for(int j=0; j < pe->cpus_count; j++)
				if(numa_bitmask_isbitset(bm, j))
				{
					pe->cpu2node[j]=i;
					pe->node_cpus[i][pe->node_cpus_length[i]++]=j;
					printf("%3d ",j);
				}

			printf("\n");
		}
		numa_free_cpumask(bm);
		bm = NULL;
		printf("\n");

	// Reading sibling groups of each node
	{
		int* sg_set = calloc(sizeof(int), pe->cpus_count);
		assert(sg_set != NULL);

		{
			char temp[128];
			long ret = run_command("cat /sys/devices/system/cpu/cpu*/topology/thread_siblings|grep -v \"^$\"|sort|uniq|wc -l", temp, 128);
			assert((int)ret == 0);
			pe->sibling_groups_count = atoi(temp);
			assert(pe->sibling_groups_count > 0);

			pe->node_sibling_groups_start_ID = calloc(sizeof(unsigned int), pe->nodes_count + 1);
			assert(pe->node_sibling_groups_start_ID != NULL);
			pe->sibling_group_cpus_start_offsets = calloc(sizeof(unsigned int), pe->sibling_groups_count + 1);
			assert(pe->sibling_group_cpus_start_offsets != NULL);
			pe->sibling_groups_cpus = calloc(sizeof(unsigned int), pe->cpus_count);
			assert(pe->sibling_groups_cpus != NULL);
		}

		int g_counter = 0; // the current number of groups
		pe->node_sibling_groups_start_ID[0] = g_counter;
		int sgc_index = 0; // the next index on `sibling_groups_cpus`
		pe->sibling_group_cpus_start_offsets[0] = sgc_index;

		for(int n = 0; n < pe->nodes_count; n++)
		{
			for(int c = 0; c < pe->node_cpus_length[n]; c++)
			{
				if(sg_set[pe->node_cpus[n][c]])
					continue;

				// Extracting cpus in this group
				{
					char fn [256];
					sprintf(fn, "/sys/devices/system/cpu/cpu%d/topology/thread_siblings", pe->node_cpus[n][c]);
					int fns = get_file_size(fn);
					char * mem = malloc(fns);
					assert(mem != NULL);
					int ret = get_file_contents(fn, mem, fns);
					assert(ret > 0);

					int last_id = 0;
					for(int i = ret - 1; i >= 0; i--)
					{
						if(mem[i] == ',' || mem[i] == 10)
							continue;

						int val = 0;
						if(mem[i] >= '0' && mem[i] <= '9')
							val = mem[i] - '0';
						else if(mem[i] >= 'a' && mem[i] <= 'f')
							val = 10 + (mem[i] - 'a');
						else
						{
							printf("%c\n",mem[i]);
							assert(0 && "Incorrect char");
						}

						while(val > 0)
						{
							int cpu_id = 0;

							if(val & 1)
								cpu_id = 0;
							else if(val & 2)
								cpu_id = 1;
							else if(val & 4)
								cpu_id = 2;
							else if(val & 8)
								cpu_id = 3;

							val -= (1U << cpu_id);
							cpu_id += last_id;

							assert(sg_set[cpu_id] == 0);
							sg_set[cpu_id] = 1;

							pe->sibling_groups_cpus[sgc_index] = cpu_id;
							sgc_index++;
						}

						last_id += 4;
					}

					free(mem);
					mem = NULL;
				}

				g_counter++;
				pe->sibling_group_cpus_start_offsets[g_counter] = sgc_index;
			}

			pe->node_sibling_groups_start_ID[n + 1] = g_counter;
		}
		assert(g_counter == pe->sibling_groups_count);
		assert(sgc_index == pe->cpus_count);
		for(int c = 0; c < pe->cpus_count; c++)
			assert(sg_set[c] == 1);
		
		free(sg_set);
		sg_set = NULL;
	}

	// Printing the sibling groups on each node
		printf("\033[1;34mSibling Groups\033[;37m:\n");
		for(unsigned int n=0; n<pe->nodes_count; n++)
		{
			printf("\033[3;34mNode %u\033[0;37m: ",n);
			for(int g = pe->node_sibling_groups_start_ID[n]; g < pe->node_sibling_groups_start_ID[n+1]; g++)
			{
				printf("[");

				for(int co = pe->sibling_group_cpus_start_offsets[g]; co < pe->sibling_group_cpus_start_offsets[g+1]; co++)
					if(co <  pe->sibling_group_cpus_start_offsets[g+1] - 1)
						printf("%u,", pe->sibling_groups_cpus[co]);
					else
						printf("%u", pe->sibling_groups_cpus[co]);

				if(g < pe->node_sibling_groups_start_ID[n+1] - 1)
					printf("], ");
				else
					printf("]");
			}

			printf("\n");
		}
		printf("\n");

	// OMP env vars
		printf("\033[1;31m%-40s\033[0;37m: %s\n","OMP_NUM_THREADS",getenv("OMP_NUM_THREADS"));
		printf("\033[1;31m%-40s\033[0;37m: %s\n","OMP_DYNAMIC",getenv("OMP_DYNAMIC"));
		printf("\033[1;31m%-40s\033[0;37m: %s\n","OMP_WAIT_POLICY",getenv("OMP_WAIT_POLICY"));
		printf("\n");

	// Initialize threads
		assert(getenv("OMP_NUM_THREADS") != NULL);
		pe->threads_count = atoi(getenv("OMP_NUM_THREADS"));
		assert(pe->threads_count > 0 && "Zero threads");
		// omp_set_num_threads(pe->threads_count);

		pe->node_threads = calloc(sizeof(unsigned int*), pe->nodes_count);
		pe->node_threads_length = calloc(sizeof(unsigned int), pe->nodes_count);
		assert(pe->node_threads != NULL && pe->node_threads_length != NULL);

		pe->thread2node = calloc(sizeof(unsigned int), pe->threads_count);
		pe->thread2cpu = calloc(sizeof(unsigned int), pe->threads_count);
		pe->threads_next_threads = calloc(sizeof(unsigned int*), pe->threads_count);
		assert(pe->thread2node != NULL && pe->thread2cpu != NULL && pe->threads_next_threads !=NULL);

		// Setting affinity of threads
		{
			int t = 0;
			int* t2c = calloc(sizeof(int), pe->threads_count);  // thread to cpu
			assert(t2c != NULL);

			unsigned int remained_cpus = pe->cpus_count;
			unsigned int remained_threads = pe->threads_count;

			for(int n = 0; n < pe->nodes_count; n++)
			{
				unsigned int node_threads = 0;
				if(n == pe->nodes_count - 1)
					node_threads = 	remained_threads;
				else
					node_threads = ceil(1.0 * remained_threads * pe->node_cpus_length[n] / remained_cpus);
				remained_cpus -= pe->node_cpus_length[n];
				remained_threads -= node_threads;

				unsigned int remained_node_threads = node_threads;
				unsigned int remained_node_cpus = pe->node_cpus_length[n];

				for(int g = pe->node_sibling_groups_start_ID[n]; g < pe->node_sibling_groups_start_ID[n+1]; g++)
				{
					unsigned int group_cpus = pe->sibling_group_cpus_start_offsets[g+1] - pe->sibling_group_cpus_start_offsets[g];
					unsigned int group_threads = 0;
					if(g == pe->node_sibling_groups_start_ID[n+1] - 1)
						group_threads = remained_node_threads;
					else
						group_threads = ceil(1.0 * remained_node_threads * group_cpus / remained_node_cpus);
					remained_node_cpus -= group_cpus;
					remained_node_threads -= group_threads;
						
					int remained_group_cpus = group_cpus;
					int remained_group_threads = group_threads;
					for(int co = pe->sibling_group_cpus_start_offsets[g]; co < pe->sibling_group_cpus_start_offsets[g+1]; co++)
					{
						int cpu_threads = 0;
						if(co ==  pe->sibling_group_cpus_start_offsets[g+1] - 1)
							cpu_threads = remained_group_threads;
						else
							cpu_threads = ceil(1.0 * remained_group_threads / remained_group_cpus);

						for(int ct = 0; ct < cpu_threads; ct++)
							t2c[t++] = pe->sibling_groups_cpus[co];

						remained_group_cpus--;
						remained_group_threads -= cpu_threads;
					}
					assert(remained_group_threads == 0);
					assert(remained_group_cpus == 0);				
				}
				assert(remained_node_threads == 0);
				assert(remained_node_cpus == 0);
			}
			assert(remained_threads == 0);
			assert(remained_cpus == 0);
			assert(t == pe->threads_count);
			
			#pragma omp parallel num_threads(pe->threads_count)
			{
				unsigned tid = omp_get_thread_num();
				assert(tid < pe->threads_count);
			
				cpu_set_t cs;
				CPU_ZERO(&cs);
				CPU_SET(t2c[tid], &cs);
				int ret=sched_setaffinity(0, sizeof(cpu_set_t), &cs);
				assert(ret == 0);
			}

			free(t2c);
			t2c = NULL;
		}
		
		// Reading affinity of threads
		#pragma omp parallel num_threads(pe->threads_count)
		{
			unsigned tid = omp_get_thread_num();
			assert(tid < pe->threads_count);
			cpu_set_t cs;
			int ret;
			CPU_ZERO(&cs);
			ret=sched_getaffinity(0, sizeof(cpu_set_t), &cs);
			if(ret)
			{
				printf("Can't get the affinity, %d, %s\n", errno, strerror(errno));
				assert(ret == 0);
			}

			for(unsigned int i = 0; i < CPU_SETSIZE; i++)
				if(CPU_ISSET(i, &cs))
				{
					unsigned int old_val = __sync_val_compare_and_swap(&pe->thread2cpu[tid], 0, i);
					assert(old_val == 0 && "Thread pinning error");
					pe->thread2node[tid] = pe->cpu2node[i];
				}
		}

		for(unsigned int n=0; n<pe->nodes_count; n++)
		{
			pe->node_threads[n] = calloc(sizeof(unsigned int), pe->threads_count);
			assert(pe->node_threads[n] != NULL);
		}

		printf("\033[1;31m%-40s\033[0;37m: ","Affinitities (tid.cpu.node)");
		int prev_node = -1;
		int indent = 1;
		for(unsigned int t=0; t<pe->threads_count; t++)
		{
			unsigned int node = pe->thread2node[t];
			pe->node_threads[node][pe->node_threads_length[node]++] = t;
			if(indent++ % 8 == 0 || prev_node != node)
			{
				prev_node = node;
				indent = 1;
				printf("\n\t");
			}
			printf("%3u.\033[0;32m%3u\033[0;37m.\033[0;33m%2u\033[0;37m, \t", t, pe->thread2cpu[t], node);
		}
		printf("\n\n");

		for(unsigned int n=0; n<pe->nodes_count; n++)
		{
			printf("Threads on node \033[1;34m%3u\033[;37m (#%3u): ", n, pe->node_threads_length[n]);
			for(unsigned int t=0; t<pe->node_threads_length[n]; t++)
				printf("%2u, ", pe->node_threads[n][t]);
			printf("\n");
		}
		printf("\n");

	// Finding next threads of each thread for work-stealing
		#pragma omp parallel num_threads(pe->threads_count)
		{
			unsigned tid = omp_get_thread_num();
			assert(tid < pe->threads_count);
			
			unsigned int thread_node = pe->thread2node[tid];
			unsigned int thread_index_in_node = -1;
			for(unsigned int ti=0; ti<pe->node_threads_length[thread_node]; ti++)
				if(pe->node_threads[thread_node][ti] == tid)
				{
					thread_index_in_node = ti;
					break;
				}
			assert(thread_index_in_node != -1);

			pe->threads_next_threads[tid] = calloc(sizeof(unsigned int), pe->threads_count);
			assert(pe->threads_next_threads[tid] != NULL);

			unsigned int nindex = 0;
			for(unsigned int rel_n=0; rel_n<pe->nodes_count; rel_n++)
			{
				unsigned int node = (thread_node + rel_n) % pe->nodes_count;
				for(unsigned int rel_t=0; rel_t<pe->node_threads_length[node]; rel_t++)
				{
					unsigned int thread_index = (rel_t + thread_index_in_node) % pe->node_threads_length[node];
					unsigned int thread = pe->node_threads[node][thread_index];
					pe->threads_next_threads[tid][nindex++] = thread;
				}
			}
			assert(nindex == pe->threads_count);
		}

		printf("\033[1;35mThread stealing order \033[;37m:\n");
		for(unsigned int t=0; t<pe->threads_count; t++)
		{
			printf("#\033[1;35m%3u\033[0;37m: ",t);

			printf("%u",pe->threads_next_threads[t][0]);
			unsigned int last = pe->threads_next_threads[t][0];
			unsigned int prev_print = pe->threads_next_threads[t][0];

			for(unsigned int t2=1; t2<pe->threads_count - 1; t2++)	
			{		
				if(last == pe->threads_next_threads[t][t2] - 1)
				{
					last++;
					continue;
				}

				if(prev_print == last)
					printf(", %u", pe->threads_next_threads[t][t2]);
				else
					printf("-%u, %u", last, pe->threads_next_threads[t][t2]);
				last = pe->threads_next_threads[t][t2];
				prev_print = last;
			}

			if(last + 1 == pe->threads_next_threads[t][pe->threads_count - 1])
				printf("-%u\n", pe->threads_next_threads[t][pe->threads_count - 1]);
			else
				printf("-%u, %u\n", last, pe->threads_next_threads[t][pe->threads_count - 1]);
		}
		printf("\n");

	// Initialzing PAPI on threads
		printf("Using \033[1;31mPAPI\033[0;37m  for measurements.\n");
		assert(sizeof(papi_events)/sizeof(papi_events[0]) <= 32);
		papi_init();
		pe->papi_args = calloc(sizeof(unsigned long), pe->threads_count);
		assert(pe->papi_args != NULL);

		#pragma omp parallel num_threads(pe->threads_count)
		{
			unsigned tid = omp_get_thread_num();
			pe->papi_args[tid] = papi_start(papi_events, sizeof(papi_events)/sizeof(papi_events[0]));
		}

		{
			unsigned long papi_arg = pe->papi_args[0];
			unsigned int event_set = (unsigned int) papi_arg;
			pe->hw_events_count = (papi_arg >> 32);
			
			unsigned int temp_count = pe->hw_events_count;
			unsigned int temp_events[32];

			int ret = PAPI_list_events(event_set, temp_events, &temp_count);
			assert(ret == PAPI_OK);
			assert(temp_count == pe->hw_events_count);

			for(unsigned int i=0; i<pe->hw_events_count; i++)
			{
				char temp[PAPI_MAX_STR_LEN];
				PAPI_event_code_to_name(temp_events[i], temp);
				sprintf(pe->hw_events_names[i],"%s",temp+5);
				// printf("%s\n", pe->hw_events_names[i]);
			}
		}

		printf("\n\n");

	return pe;
}

void numa_interleave_allocated_memory(void* addr, unsigned long mem_size)
{	
	unsigned long maxnode = 64;
	int num_nodes = numa_num_configured_nodes();
	assert(num_nodes <= maxnode);

	unsigned long nodemask = 0UL;
	for(int n=0; n < num_nodes; n++)
		nodemask += (1UL << n);
	long res = mbind(addr, mem_size, MPOL_INTERLEAVE, &nodemask, maxnode, 0);
	if(res != 0)
	{
		printf("can't mbind : %d %s\n",errno, strerror(errno));
		exit(-1);
	}

	return;
}

unsigned long get_free_mem()
{
	unsigned int nodes_count = numa_num_task_nodes();
	unsigned long total_free_mem = 0;

	for(int i=0; i<nodes_count; i++)
	{
		unsigned long free_mem = 0;
		numa_node_size(i, &free_mem);
		total_free_mem += free_mem;
	}

	return total_free_mem;
}

// This function reads the file from disk in parallel using O_DIRECT (without caching)
// It is required that main_mem has 2 * 4096 bytes more space than end_offset - start_offset

char* par_read_file(char* file_name, unsigned long start_offset, unsigned long end_offset, char* main_mem)
{
	// Checking
	{
		unsigned long file_size = get_file_size(file_name);
		assert(end_offset <= file_size);
	}

	unsigned long read_size = 4096UL * 1024 * 8 ; 
	
	if(end_offset - start_offset <= read_size)
	{
		int fd = open(file_name, O_RDONLY); 
		assert(fd > 0);

		unsigned long new_offset = lseek(fd, start_offset, SEEK_SET);
		assert(new_offset == start_offset);

		unsigned long read_bytes = 0;
		unsigned long length = end_offset - start_offset;
		while(read_bytes < length)
		{
			long ret = read(fd, main_mem + read_bytes, length - read_bytes);
			assert(ret != -1);
			read_bytes += ret;
		}
		
		close(fd);
		fd = -1;

		return main_mem;
	}

	char* mem = NULL;
	unsigned long start_bytes_before_4096 = 4096 - (start_offset % 4096);
	unsigned long remainder = ((unsigned long)main_mem +  start_bytes_before_4096) % 4096;
	if( remainder != 0)
		mem = (char*)((unsigned long)main_mem + 4096 - remainder);
	else
		mem = main_mem;

	// printf("%lu %lu", start_offset, start_bytes_before_4096);

	unsigned long total_read_bytes = 0;

	// Reading the start_bytes_before_4096
	if(start_bytes_before_4096)
	{
		int fd = open(file_name, O_RDONLY); 
		assert(fd > 0);

		unsigned long new_offset = lseek(fd, start_offset, SEEK_SET);
		assert(new_offset == start_offset);

		unsigned long read_bytes = 0;
		while(read_bytes < start_bytes_before_4096)
		{
			long ret = read(fd, mem + read_bytes, start_bytes_before_4096 - read_bytes);
			assert(ret != -1);
			read_bytes += ret;
		}
		
		total_read_bytes += read_bytes;	

		close(fd);
		fd = -1;
	}

	// Reading the blocks
	unsigned long number_of_reads = (end_offset - start_offset - total_read_bytes) / read_size;
	if(number_of_reads == 0 && end_offset - start_offset - total_read_bytes != 0)
		number_of_reads++;

	#pragma omp parallel for reduction(+:total_read_bytes)
	for(unsigned long r = 0; r< number_of_reads; r++)
	{
		unsigned long start_byte = start_offset + start_bytes_before_4096 + r * read_size;

		unsigned long length = read_size;
		int fd = -1; 
		if(r == number_of_reads - 1)
		{
			length = end_offset - start_byte;
			fd = open(file_name, O_RDONLY); 
		}
		else
			fd = open(file_name, O_RDONLY | O_DIRECT); 
		assert(fd > 0);

		unsigned long new_offset = lseek(fd, start_byte, SEEK_SET);
		assert(new_offset == start_byte);

		unsigned long read_bytes = 0;
		while(read_bytes < length)
		{
			long ret = read(fd, mem + start_byte - start_offset + read_bytes , length - read_bytes);
			assert(ret != -1);
			read_bytes += ret;
		}
		
		total_read_bytes += read_bytes;	
		close(fd);
		fd = -1;
	}

	if(total_read_bytes != end_offset - start_offset)
	{
		printf("%lu %lu\n", total_read_bytes, end_offset - start_offset);
		assert(total_read_bytes == end_offset - start_offset);
	}

	return mem;
}
#endif
