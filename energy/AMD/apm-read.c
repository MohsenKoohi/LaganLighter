/*

AMP: Application Power Management

This program reads the energy counters of AMD processors, mainly family 17h model 31h and architectures with compatible MSR numbers.
We have changed the following source code published under GNU GPL2.
https://github.com/djselbeck/rapl-read-ryzen

Other sources: 
	- https://github.com/amd/energy
	- https://github.com/deater/uarch-configure/tree/master/amd-apm-read

The Model-Specific Registers (MSR) are read to get energy unit and consumption.
We have used the following document for reading energy consumption of AMD machines:

	Preliminary Processor Programming Reference (PPR) for AMD Family 17h Model 31h, Revision B0 Processors
	https://www.amd.com/system/files/TechDocs/55803-ppr-family-17h-model-31h-b0-processors.pdf
	Document version: 55803 Rev 0.91 - Sep 1, 2020
	Pages 170 and 171

License:
	This program is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation; either version 2 of the License, or
	(at your option) any later version.

*/

#define MSR_PWR_UNIT          0xC0010299
#define MSR_CORE_ENERGY       0xC001029A
#define MSR_PACKAGE_ENERGY    0xC001029B
#define MSR_ENERGY_UNIT_MASK  0x01F00

#ifdef DEBUG
	#define printdeb(com) com
#else
	#define printdeb(com)
#endif

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <unistd.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include <time.h>

int open_msr(int core)
{
	char msr_filename[BUFSIZ];
	sprintf(msr_filename, "/dev/cpu/%d/msr", core);

	int fd = open(msr_filename, O_RDONLY);
	if ( fd < 0 ) {
		if ( errno == ENXIO ) {
			fprintf(stderr, "rdmsr: No CPU %d\n", core);
			exit(2);
		} else if ( errno == EIO ) {
			fprintf(stderr, "rdmsr: CPU %d doesn't support MSRs\n",
					core);
			exit(3);
		} else {
			perror("rdmsr:open");
			fprintf(stderr,"Could not open %s.\nThis program needs sudo access.\n\n",msr_filename);
			exit(127);
		}
	}

	return fd;
}

unsigned long read_msr(int fd, unsigned int which)
{
	unsigned long data;

	if ( pread(fd, &data, sizeof data, which) != sizeof data ) {
		perror("read_msr: pread");
		exit(127);
	}

	return data;
}

unsigned long __get_nano_time()
{
	struct timespec ts;
	timespec_get(&ts,TIME_UTC);
	return ts.tv_sec*1e9+ts.tv_nsec;
}


int main(int argc, char **argv)
{	
	unsigned long t0 = - __get_nano_time();

	// Reading the topology 
		unsigned long t1 = t0;
		unsigned int total_cores = 0;
		unsigned int* core_is_first_sibling = NULL;
		unsigned int* core_to_package = NULL;

		unsigned int total_packages = 0;
		unsigned int* package_first_core = NULL;
		{
			// Reading number of cores including SMT ones
			char* res = malloc(256);
			assert(res != NULL);
			{
				int fd = open("/sys/devices/system/cpu/online",O_RDONLY);
				assert(fd > 0);
				int ret = read(fd, res, 256);
				assert(ret > 0);
				close(fd);
				fd = -1;

				int i = 0;
				char* tres = res;
				while(*tres != '-' && i++ < ret)
					tres++;
				tres++;

				total_cores = 1 + atol(tres);

				tres = NULL;
			}	
			printdeb(printf("\ntotal_cores: %u\n", total_cores));

			// Reading core info
			core_is_first_sibling = calloc(sizeof(unsigned int), total_cores);
			assert(core_is_first_sibling != NULL);
			core_to_package = calloc(sizeof(unsigned int), total_cores);
			assert(core_to_package != NULL);
			for(unsigned int c = 0; c < total_cores; c++)
			{
				char path[256];
				char command[256];

				// Reading package
				sprintf(path, "/sys/devices/system/cpu/cpu%u/topology/physical_package_id", c);
				{
					int fd = open(path,O_RDONLY);
					assert(fd > 0);
					int ret = read(fd, res, 256);
					assert(ret > 0);
					close(fd);
					fd = -1;
				}
				core_to_package[c] = atol(res);

				if(core_to_package[c] >= total_packages)
					total_packages = core_to_package[c] + 1;

			 	// Reading siblings
			 	sprintf(path, "/sys/devices/system/cpu/cpu%u/topology/thread_siblings_list", c);
			 	int sibling_thread = 0;
				{
					int fd = open(path,O_RDONLY);
					assert(fd > 0);
					int ret = read(fd, res, 256);
					assert(ret > 0);
					close(fd);
					fd = -1;
					int i = 0;
					char* tres = res;
					while(*tres != ',' && *tres != 10 && i++ < ret)
					{
						sibling_thread = sibling_thread * 10 + (*tres - '0');
						tres++;
					}
					tres = NULL;
				}

				if(sibling_thread == c)
					core_is_first_sibling[c] = 1;
				else
				{
					core_is_first_sibling[c] = 0;
					core_to_package[c] = -1U;
				}
			}
			
			// Identifying first core of each pacakge
			package_first_core = calloc(sizeof(unsigned int), total_packages);
			assert(package_first_core != NULL);
			for(int i = 0; i < total_packages; i++)
				package_first_core[i] = -1U;
			for(int c = 0; c < total_cores; c++)
				if(core_is_first_sibling[c] == 1)
					if(package_first_core[core_to_package[c]] == -1U)
						package_first_core[core_to_package[c]] = c;

			// Printing
			printdeb(printf("total_packages: %u\n", total_packages));
			for(int p = 0; p < total_packages; p++)
				printdeb(printf("first core of package %u: %u\n", p, package_first_core[p]));
			printdeb(printf("\n(core, package, core_is_first_sibling): \n"));
			for(int c = 0; c < total_cores; c++)
			{
				printdeb(printf("(%3u, %3d,  %3d)     ", c, core_to_package[c], core_is_first_sibling[c]));
				if(c%5 == 4)
					printdeb(printf("\n"));
			}
			printdeb(printf("\n"));
		}


	// Reading the energy counters of cores
		unsigned long t2 = - __get_nano_time();
		t1 += - t2;

		double* package_core_energies = calloc(sizeof(double), total_packages);
		assert(package_core_energies != NULL);
		double energy_unit = .0;
		double max_energy = 0;

		for (unsigned int c = 0; c < total_cores; c++)
		{
			if(core_is_first_sibling[c] != 1)
				continue;
			assert(core_to_package[c] != -1U);
			assert(core_to_package[c] < total_packages);

			int fd = open_msr(c);	
			if(c == 0)
			{
				unsigned long temp = read_msr(fd, MSR_PWR_UNIT);
				temp = (temp & MSR_ENERGY_UNIT_MASK) >> 8;				
				energy_unit = pow(0.5, (double)temp);
				unsigned int me = -1U;
				max_energy =  me * energy_unit;
				printdeb(printf("\nEnergy_unit: %.10f\n", energy_unit));
				printdeb(printf("Max_energy: %.10f\n\n", max_energy));


				printf("%-5s; %-20s; %-20s;\n","core","energy_joules","max_energy_joules");
			}
			unsigned int core_energy_raw = (unsigned int)read_msr(fd, MSR_CORE_ENERGY);
			close(fd);

			double core_energy = core_energy_raw * energy_unit;
			// package_core_energies[core_to_package[c]] += core_energy;

			printf("core-%u; %20.10f; %20.10f;\n", c,  core_energy, max_energy);
		}
		printf("\n");

		printf("%-8s; %-20s; %-20s;\n","package","package_joules","max_package_energy_joules");
		for (unsigned int p = 0; p < total_packages; p++)
		{
			if(package_first_core[p] == -1U)
				continue;

			unsigned int core = package_first_core[p];
			int fd = open_msr(core);	

			unsigned int package_energy_raw = (unsigned int) read_msr(fd, MSR_PACKAGE_ENERGY);
			close(fd);

			double package_energy = package_energy_raw * energy_unit;
			printf("package-%u; %20.10f; %20.10f;\n", p, package_energy, max_energy);
		}

		t2 += __get_nano_time();

	// Releasing mem
		free(core_is_first_sibling);
		core_is_first_sibling = NULL;

		free(core_to_package);
		core_to_package = NULL;

		free(package_first_core);
		package_first_core = NULL;

		free(package_core_energies);
		package_core_energies = NULL;
	
	t0 += 	__get_nano_time();
	printdeb(printf("\nTiming:\n  S0: %.3f; S1: %.3f; Total: %.3f (ms)\n", t1/1e9, t2/1e9, t0/1e9));

	return 0;
}
