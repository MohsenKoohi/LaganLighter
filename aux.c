#ifndef __AUX_C
#define __AUX_C

#ifndef _BSD_SOURCE
	#define _BSD_SOURCE
#endif
#define _GNU_SOURCE

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <asm/unistd.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <linux/hw_breakpoint.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <linux/futex.h>
#include <emmintrin.h>
#include <math.h>
#include <curl/curl.h>
#include <sys/types.h>
#include <sys/stat.h>

#define max(a,b)             \
({                           \
    __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a > _b ? _a : _b;       \
})

#define min(a,b)             \
({                           \
    __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a < _b ? _a : _b;       \
})


int get_host_name(char* input,int in_size)
{
	int fd=open("/proc/sys/kernel/hostname", O_RDONLY);
	if(fd<0)
	{
		printf("Can't open the /proc/sys/kernel/hostname : %d - %s\n",errno,strerror(errno));
		return 0;
	}

	int count = read(fd, input, in_size);
	if(count<in_size)
		input[count]=0;

	close(fd);

	return count;
}

unsigned long get_nano_time()
{
	struct timespec ts;
	timespec_get(&ts,TIME_UTC);
	return ts.tv_sec*1e9+ts.tv_nsec;
}

void quick_sort_uint(unsigned int* bucket, unsigned long start, unsigned long end)
{
	if(end - start < 16)
	{
		do{
			unsigned long new_end = start;

			for(unsigned long i=start+1; i<=end; i++)	
				if(bucket[i-1] > bucket[i])
				{
					unsigned int t=bucket[i];
					bucket[i]=bucket[i-1];
					bucket[i-1]=t;

					new_end = i;
				}

			end = new_end;
		}
		while(end > start);

		return;
	}

	// moving the middle index to the end to suffle for semi-sorted arrays
	unsigned int t=bucket[(end+start)/2];
	bucket[(end+start)/2] = bucket[end];
	bucket[end] = t;

	unsigned long pivot_index=end;
	unsigned long front_index=start;

	unsigned int bp;
	unsigned int p=bucket[pivot_index];
	unsigned int f;

	while(pivot_index > front_index)
	{
		//printf("%2d %2d ** \t",pivot_index, front_index);
		if(bucket[front_index] >= p)
		{
			bp=bucket[pivot_index-1];
			f=bucket[front_index];

			bucket[pivot_index]=f;
			bucket[front_index]=bp;

			pivot_index--;
			
		}
		else
			front_index++;
	}

	bucket[pivot_index]=p;

	if(pivot_index > start + 1)
		quick_sort_uint(bucket, start, pivot_index-1);

	if(pivot_index < end - 1)
		quick_sort_uint(bucket, pivot_index+1, end);

	return;
}

void quick_sort_ushort(unsigned short* bucket, unsigned long start, unsigned long end)
{
	if(end - start < 16)
	{
		do{
			unsigned long new_end = start;

			for(unsigned long i=start+1; i<=end; i++)	
				if(bucket[i-1] > bucket[i])
				{
					unsigned short t=bucket[i];
					bucket[i]=bucket[i-1];
					bucket[i-1]=t;

					new_end = i;
				}

			end = new_end;
		}
		while(end > start);

		return;
	}

	// moving the middle index to the end to suffle for semi-sorted arrays
	unsigned short t=bucket[(end+start)/2];
	bucket[(end+start)/2] = bucket[end];
	bucket[end] = t;

	unsigned long pivot_index=end;
	unsigned long front_index=start;

	unsigned short bp;
	unsigned short p=bucket[pivot_index];
	unsigned short f;

	while(pivot_index > front_index)
	{
		//printf("%2d %2d ** \t",pivot_index, front_index);
		if(bucket[front_index] >= p)
		{
			bp=bucket[pivot_index-1];
			f=bucket[front_index];

			bucket[pivot_index]=f;
			bucket[front_index]=bp;

			pivot_index--;
			
		}
		else
			front_index++;
	}

	bucket[pivot_index]=p;

	if(pivot_index > start + 1)
		quick_sort_ushort(bucket, start, pivot_index-1);

	if(pivot_index < end - 1)
		quick_sort_ushort(bucket, pivot_index+1, end);

	return;
}

char* get_date_time(char* in)
{
	time_t t0 = time(NULL);
	struct tm * t = localtime(&t0);
	sprintf(in, "%04d/%02d/%02d %02d:%02d:%02d", 1900 + t->tm_year, t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
	return in;
}

unsigned long get_file_size(char* input_file_name)
{
	int fd = open(input_file_name,O_RDONLY); 
	assert(fd > 0);

	struct stat st;
	assert(0 == fstat(fd, &st));
	close(fd);
	fd = -1;
	
	return st.st_size;
}


int get_file_contents(char* file_name, char* buff, int buff_size)
{
	int fd=open(file_name, O_RDONLY);
	if(fd<0)
	{
		printf("Can't open the %s : %d - %s\n",file_name, errno, strerror(errno));
		return -1;
	}

	int count = read(fd, buff, buff_size);
	if(count<buff_size)
		buff[count]=0;

	close(fd);

	return count;
}


// Binary Search for `val` in [`start`, `end`) of `vals` as a sorted array  
// returns -1UL if it cannot find
// otherwise, returns index of the element that is equal to val
unsigned long uint_binary_search(unsigned int* vals, unsigned long in_start, unsigned long in_end, unsigned int val)
{
	unsigned long end = in_end;
	unsigned long start = in_start;
	assert(start <= end);
	if(start == end)
		return -1UL;

	if(vals[start] > val) 
		return -1UL;

	if(vals[end - 1] < val)
		return -1UL; 

	unsigned long m = (start + end)/2;
	unsigned int r = 0;
	unsigned int r_max = 1 + log2(end + 1 - start);
	while(1)
	{
		unsigned int m_val = vals[m];
		if( m_val == val )
			return m;

		if(m + 1 < in_end)
		{
			if(vals[m+1] == val)
				return m+1;

			if(m_val < val && vals[m + 1] > val)
				return -1UL;
		}

		if(m_val > val)
			end = m;
		if(m_val < val)
			start = m;
		m = (start + end)/2;

		assert(r++ <= r_max);
	}

	assert("Don't reach here.");
	return -1UL;
}

#endif