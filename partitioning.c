#ifndef __PARTITIONING_C
#define __PARTITIONING_C

int serial_edge_partitioning(struct ll_400_graph* g, unsigned int* borders, int partitions)
{
	assert(partitions > 0 && g->vertices_count > 0);

	borders[0] = 0;
	borders[partitions] = g->vertices_count;

	if(g->vertices_count <= 4 * partitions)
	{
		unsigned int remained_vertices = g->vertices_count;
		unsigned int remained_partitions = partitions;
		for(int i=1;i<partitions;i++)
		{
			if(remained_vertices > 0)
			{
				unsigned int q = max(1,remained_vertices/remained_partitions);
				borders[i] = borders[i-1] + q;
				remained_vertices -= q;
			}
			else
				borders[i] = g->vertices_count;

			//printf("%d %d %d\n",i,borders[i], remained_vertices);
			remained_partitions--;
		}
	}
	else
	{
		unsigned long edges_per_thread = (g->edges_count + g->vertices_count) / partitions;
		unsigned int last_m = 0;
		for(unsigned int t = 1; t < partitions; t++)
		{
			unsigned long start = 0;
			unsigned long end = g->vertices_count;
			unsigned long target = t * edges_per_thread;
			unsigned long m = (start + end)/2;
			while(1)
			{
				unsigned long m_val = g->offsets_list[m] + m;
				if( m_val == target )
					break;

				unsigned long b_val = g->offsets_list[m - 1] + m - 1;
				if( b_val < target && m_val > target )
					break;

				if(m_val > target)
					end = m;
				if(m_val < target)
					start = m;

				unsigned long new_m = (start + end)/2;
				if(new_m == m)
					break;
				m = new_m;
			}
			assert( m <= g->vertices_count );

			if(m <= last_m)
				m = last_m + 1;
			if(m > g->vertices_count)
				m = g->vertices_count;
			
			borders[t] = m;
			last_m = m;
		}
	}

	//verify_serial_edge_partitioning(g, borders, partitions);

	return 0;
}

int parallel_edge_partitioning(struct ll_400_graph* g, unsigned int* borders, int partitions)
{
	assert(partitions > 0 && g->vertices_count > 0);

	borders[0] = 0;
	borders[partitions] = g->vertices_count;

	if(g->vertices_count <= 4 * partitions)
	{
		unsigned int remained_vertices = g->vertices_count;
		unsigned int remained_partitions = partitions;
		for(int i=1;i<partitions;i++)
		{
			if(remained_vertices > 0)
			{
				unsigned int q = max(1,remained_vertices/remained_partitions);
				borders[i] = borders[i-1] + q;
				remained_vertices -= q;
			}
			else
				borders[i] = g->vertices_count;

			//printf("%d %d %d\n",i,borders[i], remained_vertices);
			remained_partitions--;
		}
	}
	else
	{
		unsigned long edges_per_thread = (g->edges_count + g->vertices_count) / partitions;
		
		#pragma omp parallel for
		for(unsigned int t = 1; t < partitions; t++)
		{
			unsigned long start = 0;
			unsigned long end = g->vertices_count;
			unsigned long target = t * edges_per_thread;
			unsigned long m = (start + end)/2;
			while(1)
			{
				unsigned long m_val = g->offsets_list[m] + m;
				if( m_val == target )
					break;

				unsigned long b_val = g->offsets_list[m - 1] + m - 1;
				if( b_val < target && m_val > target )
					break;

				if(m_val > target)
					end = m;
				if(m_val < target)
					start = m;

				unsigned long new_m = (start + end)/2;
				if(new_m == m)
					break;
				m = new_m;
			}
			assert( m <= g->vertices_count );
			
			borders[t] = m;
		}	

		unsigned int last_m = 0;
		for(unsigned int t = 1; t < partitions; t++)
		{
			if(borders[t] <= last_m)
				borders[t] = last_m + 1;
			if(borders[t] > g->vertices_count)
				borders[t] = g->vertices_count;
			
			last_m = borders[t];
		}
	}

	//verify_serial_edge_partitioning(g, borders, partitions);

	return 0;
}

struct dynamic_partitioning
{
	struct par_env* pe;

	unsigned int partitions_count;
	unsigned int partitions_remained;
	unsigned int threads_count;

	unsigned int* threads_partitions_start;
	unsigned int* threads_partitions_end;
	unsigned int* threads_partitions_remained;

	unsigned int* threads_last_steal_offset;
	
	unsigned char* partitions_status;
};

struct dynamic_partitioning* dynamic_partitioning_initialize(struct par_env* pe, unsigned int partitions_count)
{
	assert(partitions_count > 0 && pe != NULL);
	
	struct dynamic_partitioning* dp = calloc(sizeof(struct dynamic_partitioning), 1);
	assert(dp != NULL);
	dp->partitions_count = partitions_count;
	dp->pe = pe;
	dp->threads_count = pe->threads_count;

	dp->threads_partitions_start = calloc(pe->threads_count ,sizeof(unsigned int));
	dp->threads_partitions_end = calloc(pe->threads_count ,sizeof(unsigned int));
	dp->threads_partitions_remained = calloc(pe->threads_count,sizeof(unsigned int));
	assert(dp->threads_partitions_start != NULL && dp->threads_partitions_end != NULL && dp->threads_partitions_remained != NULL);

	dp->threads_last_steal_offset = calloc(pe->threads_count ,sizeof(unsigned int));
	assert(dp->threads_last_steal_offset != NULL);

	dp->partitions_status = calloc(dp->partitions_count, sizeof(unsigned char));
	assert(dp->partitions_status != NULL);

	dp->partitions_remained = partitions_count;

	// Partitioning for nodes and threads
	unsigned int remained_threads = pe->threads_count;
	unsigned int remained_partitions = dp->partitions_count;
	unsigned int offset = 0;

	for(int t=0; t<pe->threads_count; t++)
	{	
		dp->threads_partitions_start[t] = offset;
		unsigned int quota = remained_partitions/remained_threads;
		offset += quota;
		dp->threads_partitions_end[t] = offset;
		dp->threads_partitions_remained[t] = quota;

		remained_threads--;
		remained_partitions -= quota;

		dp->threads_last_steal_offset[t] = 0;

		// printf("\t\tPartitions for thread %u: %'u - %'u\n",t, dp->threads_partitions_start[t], dp->threads_partitions_end[t]);
	}

	return dp;
}


void dynamic_partitioning_release(struct dynamic_partitioning* dp)
{
	assert(dp != NULL);
	dp->pe = NULL;

	free(dp->threads_partitions_start);
	dp->threads_partitions_start = NULL;

	free(dp->threads_partitions_end);
	dp->threads_partitions_end = NULL;

	free(dp->threads_partitions_remained);
	dp->threads_partitions_remained = NULL;

	free(dp->partitions_status);
	dp->partitions_status = NULL;

	free(dp->threads_last_steal_offset);
	dp->threads_last_steal_offset = NULL;

	free(dp);

	return;
}

void dynamic_partitioning_reset(struct dynamic_partitioning* dp)
{
	assert(dp->partitions_remained == 0);
	dp->partitions_remained =  dp->partitions_count;
	
	#pragma omp parallel for
	for(unsigned int i=0; i<dp->partitions_count; i++)
	{
		assert(dp->partitions_status[i] == 1);
		dp->partitions_status[i] = 0;
	}

	for(unsigned int t=0; t<dp->threads_count; t++)
	{
		assert(dp->threads_partitions_remained[t] == 0);
		dp->threads_partitions_remained[t] =  dp->threads_partitions_end[t] - dp->threads_partitions_start[t];
		dp->threads_last_steal_offset[t] = 0;
	}

	return;
}


unsigned int dynamic_partitioning_get_next_partition(struct dynamic_partitioning* dp, unsigned int thread_id, unsigned int prev_partition)
{
	#define CHECK_AND_GET_PARTITION(__p, __thread_id) \
		if(__sync_bool_compare_and_swap(&dp->partitions_status[__p], 0, 1))	\
		{ \
			while(1) \
			{ \
				unsigned int temp = __atomic_load_n(&dp->partitions_remained, __ATOMIC_SEQ_CST); \
				if(__sync_bool_compare_and_swap(&dp->partitions_remained, temp, temp-1)) \
					break; \
			} \
			while(1) \
			{ \
				unsigned int temp = __atomic_load_n(&dp->threads_partitions_remained[__thread_id], __ATOMIC_SEQ_CST); \
				if(__sync_bool_compare_and_swap(&dp->threads_partitions_remained[__thread_id], temp, temp-1)) \
					break; \
			} \
			return p; \
		}

	if( __atomic_load_n(&dp->partitions_remained, __ATOMIC_SEQ_CST) == 0)
		return -1U;

	unsigned int start_partition = prev_partition + 1;
	if(prev_partition == -1U)
	{
		start_partition = dp->threads_partitions_start[thread_id];
		dp->threads_last_steal_offset[thread_id] = 0;
	}

	while(dp->threads_last_steal_offset[thread_id] < dp->threads_count)
	{
		unsigned int target_thread_id = dp->pe->threads_next_threads[thread_id][dp->threads_last_steal_offset[thread_id]];

		if( __atomic_load_n(&dp->threads_partitions_remained[target_thread_id], __ATOMIC_SEQ_CST) == 0)
		{
			dp->threads_last_steal_offset[thread_id]++;
			continue;
		}

		if(target_thread_id == thread_id)
		{
			// We are processing our partitions therefore go in the ascending order
			for(unsigned int p=start_partition; p<dp->threads_partitions_end[target_thread_id]; p++)
				CHECK_AND_GET_PARTITION(p, target_thread_id);
		}
		else
		{
			start_partition = prev_partition - 1;
			// We are processing partitions of other threads, so we start from the last partition of each victim thread
			// To prevent interrupting normal order of the victim

			if(start_partition >= dp->threads_partitions_end[target_thread_id] || start_partition < dp->threads_partitions_start[target_thread_id])
				start_partition = dp->threads_partitions_end[target_thread_id] - 1;

			for(unsigned int p=start_partition; p>=dp->threads_partitions_start[target_thread_id] && p != -1U; p--)
				CHECK_AND_GET_PARTITION(p, target_thread_id);
		}

		dp->threads_last_steal_offset[thread_id]++;
	}	
	
	return -1U;
}

#endif 