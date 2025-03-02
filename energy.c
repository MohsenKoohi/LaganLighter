#ifdef _ENERGY_MEASUREMENT

#ifndef __ENERGY_C
#define __ENERGY_C

#include "aux.c"

#include <pthread.h>
#include <regex.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <signal.h>

#define _MAX_SOCKETS 64
#define _MAX_CORES 4096
// #define _EM_DEBUG 1
#define _EM_DEBUG 0
// #define _EM_DEBUG_2 1
#define _EM_DEBUG_2 0

// Energy values are in _ Joules 
struct energy_counters_vals
{
	unsigned long socket_packages[_MAX_SOCKETS];
	unsigned long socket_cores[_MAX_SOCKETS];
	unsigned long socket_drams[_MAX_SOCKETS];
	unsigned long cores [_MAX_CORES];
	unsigned long packages_total;
	unsigned long drams_total;
};

struct energy_measurement
{
	struct utsname uts;
	int cpu_brand;   // 1 -> intel, 2-> amd
	char read_counters_command[512];
	struct energy_counters_vals last;
	struct energy_counters_vals diff;
	pthread_t thread_id;
	int thread_tid;
	int thread_stop;
	int thread_started;
	int thread_sleep_microseconds;
	char* raw_counters;
	int raw_counters_size;
	int matches_count;
	regmatch_t* matches;
};

struct energy_measurement* __em = NULL;

void __energy_measurement_read_counter_intel(struct energy_measurement* em, int reset_0_update_1)
{	
		unsigned long _tr = - get_nano_time();
	int ret = run_command(em->read_counters_command, em->raw_counters, em->raw_counters_size);
		_tr += get_nano_time();
		_EM_DEBUG_2 && printf("  Call exec time: %.3f (s)\n", _tr/1e9);
	
	if(ret < 0)
		return;

	if(_EM_DEBUG)
		printf("%s\n",em->raw_counters);

	// Reading package vals
	{
		regex_t reegex;
		ret = regcomp(&reegex, "package-([0-9]+)\\s*;\\s*([0-9]+);\\s*([0-9]+)", REG_EXTENDED);
		assert(ret == 0 && "Error in regcomp");

		int offset = 0;
		while(1)
		{
			ret = regexec(&reegex, em->raw_counters + offset, em->matches_count, em->matches, 0);
			if(ret != 0)
				break;

			// for(int i = 0; i< 4; i++)
			// 	printf("  %d %d\n", em->matches[i].rm_so, em->matches[i].rm_eo);

			int socket = atol(&em->raw_counters[offset + em->matches[1].rm_so]);
			unsigned long energy_uj = atol(&em->raw_counters[offset + em->matches[2].rm_so]);
			unsigned long max_energy_uj = atol(&em->raw_counters[offset + em->matches[3].rm_so]); 

			if(reset_0_update_1 == 1)
			{
				if(em->last.socket_packages[socket] > energy_uj)
					em->diff.socket_packages[socket] += energy_uj + max_energy_uj - em->last.socket_packages[socket];
				else
					em->diff.socket_packages[socket] += energy_uj - em->last.socket_packages[socket];
			}
			else
				em->diff.socket_packages[socket] = 0;
					
			em->last.socket_packages[socket] = energy_uj;

			if(_EM_DEBUG)
				printf("    package -> socket:%'3d, energy:%'15lu, max:%'15lu, diff: %'15lu\n", socket, energy_uj, max_energy_uj, em->diff.socket_packages[socket]);

			offset += em->matches[0].rm_eo;
		}
	}

	// Reading core vals
	{
		regex_t reegex;
		ret = regcomp(&reegex, "([0-9]+):[0-9]+\\s*;\\s*core\\s*;\\s*([0-9]+);\\s*([0-9]+)", REG_EXTENDED);
		assert(ret == 0 && "Error in regcomp");

		int offset = 0;
		while(1)
		{
			ret = regexec(&reegex, em->raw_counters + offset, em->matches_count, em->matches, 0);
			if(ret != 0)
				break;

			// for(int i = 0; i< 4; i++)
			// 	printf("  %d %d\n", em->matches[i].rm_so, em->matches[i].rm_eo);

			int socket = atol(&em->raw_counters[offset + em->matches[1].rm_so]);
			unsigned long energy_uj = atol(&em->raw_counters[offset + em->matches[2].rm_so]);
			unsigned long max_energy_uj = atol(&em->raw_counters[offset + em->matches[3].rm_so]); 

			if(reset_0_update_1 == 1)
			{
				if(em->last.socket_cores[socket] > energy_uj)
					em->diff.socket_cores[socket] += energy_uj + max_energy_uj - em->last.socket_cores[socket];
				else
					em->diff.socket_cores[socket] += energy_uj - em->last.socket_cores[socket];
			}
			else
				em->diff.socket_cores[socket] = 0;
					
			em->last.socket_cores[socket] = energy_uj;

			if(_EM_DEBUG)
				printf("    core    -> socket:%'3d, energy:%'15lu, max:%'15lu, diff: %'15lu\n", socket, energy_uj, max_energy_uj, em->diff.socket_cores[socket]);

			offset += em->matches[0].rm_eo;
		}
	}

	// Reading dram vals
	{
		regex_t reegex;
		ret = regcomp(&reegex, "([0-9]+):[0-9]+\\s*;\\s*dram\\s*;\\s*([0-9]+);\\s*([0-9]+)", REG_EXTENDED);
		assert(ret == 0 && "Error in regcomp");

		int offset = 0;
		while(1)
		{
			ret = regexec(&reegex, em->raw_counters + offset, em->matches_count, em->matches, 0);
			if(ret != 0)
				break;

			// for(int i = 0; i< 4; i++)
			// 	printf("  %d %d\n", em->matches[i].rm_so, em->matches[i].rm_eo);

			int socket = atol(&em->raw_counters[offset + em->matches[1].rm_so]);
			unsigned long energy_uj = atol(&em->raw_counters[offset + em->matches[2].rm_so]);
			unsigned long max_energy_uj = atol(&em->raw_counters[offset + em->matches[3].rm_so]); 

			if(reset_0_update_1 == 1)
			{
				if(em->last.socket_drams[socket] > energy_uj)
					em->diff.socket_drams[socket] += energy_uj + max_energy_uj - em->last.socket_drams[socket];
				else
					em->diff.socket_drams[socket] += energy_uj - em->last.socket_drams[socket];
			}
			else
				em->diff.socket_drams[socket] = 0;
					
			em->last.socket_drams[socket] = energy_uj;

			if(_EM_DEBUG)
				printf("    drams   -> socket:%'3d, energy:%'15lu, max:%'15lu, diff: %'15lu\n", socket, energy_uj, max_energy_uj, em->diff.socket_drams[socket]);

			offset += em->matches[0].rm_eo;
		}
	}

	if(_EM_DEBUG)
		printf("\n");

	return;
}

void __energy_measurement_read_counter_amd(struct energy_measurement* em, int reset_0_update_1)
{
		unsigned long _tr = - get_nano_time();
	int ret = run_command(em->read_counters_command, em->raw_counters, em->raw_counters_size);
		_tr += get_nano_time();
		_EM_DEBUG_2 && printf("  Call exec time: %.3f (s)\n", _tr/1e9);
	
	if (ret < 0)
		return;

	if(_EM_DEBUG)
		printf("%s\n",em->raw_counters);

	// Reading package vals
	{
		regex_t reegex;
		ret = regcomp(&reegex, "core-([0-9]+);\\s*([0-9]+\\.[0-9]+);\\s*([0-9]+\\.[0-9]+);", REG_EXTENDED);
		assert(ret == 0 && "Error in regcomp");

		int offset = 0;
		while(1)
		{
			ret = regexec(&reegex, em->raw_counters + offset, em->matches_count, em->matches, 0);
			if(ret != 0)
				break;

			// for(int i = 0; i< 3; i++)
			// 	printf("  %d %d\n", em->matches[i].rm_so, em->matches[i].rm_eo);

			int core = atol(&em->raw_counters[offset + em->matches[1].rm_so]);
			unsigned long energy_uj = 1e6 * atof(&em->raw_counters[offset + em->matches[2].rm_so]);
			unsigned long max_energy_uj = 1e6 * atof(&em->raw_counters[offset + em->matches[3].rm_so]);

			if(reset_0_update_1 == 1)
			{
				if(em->last.cores[core] > energy_uj)
					em->diff.cores[core] += energy_uj + max_energy_uj - em->last.cores[core];
				else
					em->diff.cores[core] += energy_uj - em->last.cores[core];
			}
			else
				em->diff.cores[core] = 0;
					
			em->last.cores[core] = energy_uj;

			if(_EM_DEBUG)
				printf("    core:%'3d, energy:%'15lu, max_en: %'15lu, diff: %'15lu\n", core, energy_uj, max_energy_uj, em->diff.cores[core]);

			offset += em->matches[0].rm_eo;
		}
	}

	// Reading core vals
	{
		regex_t reegex;
		ret = regcomp(&reegex, "package-([0-9]+);\\s*([0-9]+\\.[0-9]+);\\s*([0-9]+\\.[0-9]+);", REG_EXTENDED);
		assert(ret == 0 && "Error in regcomp");

		int offset = 0;
		while(1)
		{
			ret = regexec(&reegex, em->raw_counters + offset, em->matches_count, em->matches, 0);
			if(ret != 0)
				break;

			// for(int i = 0; i< 4; i++)
			// 	printf("  %d %d\n", em->matches[i].rm_so, em->matches[i].rm_eo);

			int socket = atol(&em->raw_counters[offset + em->matches[1].rm_so]);
			unsigned long package_energy_uj = 1e6 * atof(&em->raw_counters[offset + em->matches[2].rm_so]);
			unsigned long max_energy_uj = 1e6 * atof(&em->raw_counters[offset + em->matches[3].rm_so]); 

			if(reset_0_update_1 == 1)
			{
				if(em->last.socket_packages[socket] > package_energy_uj)
					em->diff.socket_packages[socket] += package_energy_uj + max_energy_uj - em->last.socket_packages[socket];
				else
					em->diff.socket_packages[socket] += package_energy_uj - em->last.socket_packages[socket];
			}
			else
				em->diff.socket_packages[socket] = 0;
					
			em->last.socket_packages[socket] = package_energy_uj;

			if(_EM_DEBUG)
				printf("    package -> socket:%'3d, package:%'15lu, max_en:%'15lu,  diff: %'15lu\n", socket, package_energy_uj, max_energy_uj, em->diff.socket_packages[socket]);

			offset += em->matches[0].rm_eo;
		}
	}

	if(_EM_DEBUG)
		printf("\n");

	return;
}

struct energy_measurement* energy_measurement_init()
{
	assert(numa_num_configured_cpus() <= _MAX_CORES);
	assert(numa_num_task_nodes() <= _MAX_SOCKETS);

	struct energy_measurement* em = calloc(sizeof(struct energy_measurement), 1);
	assert(em != NULL);

	em->raw_counters_size = 128 * 1024;
	em->raw_counters = calloc(sizeof(char), em->raw_counters_size);
	assert(em->raw_counters != NULL);

	em->matches_count = 16;
	em->matches = calloc(sizeof(regmatch_t), em->matches_count);
	assert(em->matches != NULL);

	char cpu_brand[16] = {0};
	{
		unsigned int cpuid_max_eax = 0;
		unsigned int* ebx = (unsigned int*)&cpu_brand[0];
		unsigned int* ecx = (unsigned int*)&cpu_brand[8];
		unsigned int* edx = (unsigned int*)&cpu_brand[4];
		cpu_brand[13]=0;
		__cpuid_count(0, 0, cpuid_max_eax, *ebx, *ecx, *edx);
	}
	if(!strcmp("GenuineIntel",cpu_brand))
		em->cpu_brand = 1;
	else if(!strcmp("AuthenticAMD",cpu_brand))
		em->cpu_brand = 2;
	else
		assert("CPU brand not detected.");

	int ret = uname(&em->uts);
	assert(ret == 0);
	// printf("  System is %s on %s hardware\n", em->uts.sysname, em->uts.machine);
	// printf("  OS Release is %s %u\n", em->uts.release);
	// printf("  OS Version is %s\n", em->uts.version);
	assert(!strcmp(em->uts.sysname,"Linux"));

	// em->cpu_brand = 2;

	if(em->cpu_brand == 1)
	{	
		if(atoi(em->uts.release) <= 4 )
			// Kelvin Intel
			sprintf(em->read_counters_command, "energy/Intel/rapl_read.sh");
		else
			// HPDC Intel
			{
				sprintf(em->read_counters_command, "sudo /var/shared/power/bin/rapl_read.sh");
				
				// int ret = run_command("sudo /var/shared/power/bin/enable-perf.sh", NULL, 0);
				// assert(ret == 0);
			}
	}
	else if(em->cpu_brand == 2)
	{
		// Kelvin AMD 
			sprintf(em->read_counters_command, "sudo /opt/service/bin/apm-read");
			// sprintf(em->read_counters_command, "cat AMD/sample_exec.txt");
	}
	printf("  Energy counters command: \"%s\"\n", em->read_counters_command);

	em->thread_sleep_microseconds = 60 * 1000 * 1000 ;
	printf("  Thread sleep time: %'.3f(s)\n", em->thread_sleep_microseconds/1e6);

	return em;
}

void __thread_sig_handler(int signum)
{
	if(signum != -1U && signum != SIGUSR1)
		return;

	_EM_DEBUG_2 && printf("\n  tid: %d Signal %d received\n", syscall(SYS_gettid), signum );

	if(__em->cpu_brand == 1)
		__energy_measurement_read_counter_intel(__em, 1);
	else if(__em->cpu_brand == 2)
		__energy_measurement_read_counter_amd(__em, 1);

	return;
}


void* __energy_measurement_thread(void* in_em)
{
	__em = (struct energy_measurement*)in_em;

	// Reseting counters
	{
		for(int s = 0; s < _MAX_SOCKETS; s++)
		{
			__em->diff.socket_cores[s] = 0;
			__em->diff.socket_drams[s] = 0;
			__em->diff.socket_packages[s] = 0;

			__em->last.socket_cores[s] = 0;
			__em->last.socket_drams[s] = 0;
			__em->last.socket_packages[s] = 0;
		}

		for(int c = 0; c < _MAX_CORES; c++)
		{
			__em->diff.cores[c] = 0;
			__em->last.cores[c] = 0;
		}
	}

	// Reading last vals
	if(__em->cpu_brand == 1)
		__energy_measurement_read_counter_intel(__em, 0);
	else if(__em->cpu_brand == 2)
		__energy_measurement_read_counter_amd(__em, 0);

	// Setting signal handler
	void (*old_handler)(int) = signal(SIGUSR1, __thread_sig_handler);
	if(old_handler == SIG_ERR)
	{
		printf("  Error: Can't initialize signal.\n");
		return NULL;
	}

	__em->thread_tid = syscall(SYS_gettid);
	__em->thread_started = 1;

	// Loop
	while(__em->thread_stop == 0)
	{
		usleep(__em->thread_sleep_microseconds);

		if(!__em->thread_stop)
			__thread_sig_handler(-1U);
	}

	__em = NULL;

	return NULL;
}

void energy_measurement_start(struct energy_measurement* em)
{
	em->thread_stop = 0;
	em->thread_started = 0;
	
	int ret = pthread_create(&em->thread_id, NULL, __energy_measurement_thread, (void*) em);
	assert(ret == 0);

	while(em->thread_started == 0)
		usleep(1000);

	return;
}

struct energy_counters_vals* energy_measurement_stop(struct energy_measurement* em)
{
	unsigned long t0 = -get_nano_time();
	em->thread_stop = 1;

	int ret = kill(em->thread_tid, SIGUSR1);
	assert(ret == 0);

	ret = pthread_join(em->thread_id, NULL);
	assert(ret == 0);
	t0  += get_nano_time();
	_EM_DEBUG_2 && printf("  Wait time for joining the thread: %.3f seconds.\n", t0/1e9);

	em->diff.packages_total = 0;
	em->diff.drams_total = 0;
	for(int s = 0; s < _MAX_SOCKETS; s++)
	{
		// if(em->diff.socket_packages[s] == 0)
		// 	continue;
		
		em->diff.packages_total += em->diff.socket_packages[s];
		em->diff.drams_total += em->diff.socket_drams[s];
	}

	return &em->diff;
}

void energy_measurement_print(struct energy_counters_vals* val)
{
	printf("Total consumed energy: %'.6f (J)\n", (val->packages_total + val->drams_total)/1e6);

	for(int s = 0; s < _MAX_SOCKETS; s++)
	{
		if(val->socket_packages[s] == 0)
			continue;
		printf("  Socket: %2d; Package: %'.6f (J); ", s, val->socket_packages[s]/1e6);
		if(val->socket_cores[s])
			printf("  Cores : %'.6f (J);", val->socket_cores[s]/1e6);
		if(val->socket_drams[s])
			printf("  DRAM : %'.6f (J);", val->socket_drams[s]/1e6);
		printf("\n");
	}

	if(_EM_DEBUG)
		for(int c = 0; c < _MAX_CORES; c++)
		{
			if(val->cores[c] == 0)
				continue;
			printf("  Core: %2d; Energy: %'.6f (J);\n", c, val->cores[c]/1e6);
		}

	return;
}

void energy_measurement_release(struct energy_measurement* em)
{
	assert(em != NULL);

	memset(em->raw_counters, 0, sizeof(char) * em->raw_counters_size);	
	free(em->raw_counters);
	em->raw_counters = NULL;

	memset(em->matches, 0, sizeof(regmatch_t) * em->matches_count);	
	free(em->matches);
	em->matches = NULL;

	memset(em, 0, sizeof(struct energy_measurement));	
	free(em);
	em = NULL;

	return;	
}

#endif

#endif