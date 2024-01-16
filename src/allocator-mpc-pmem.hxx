
#pragma once

#include <mpcalloc/sctk_alloc_pmem.h>

#if defined(PMDK_SUPPORTED)
# include <libpmem.h>
#else
# include <string.h>
#endif

#include "allocator.hxx"

class AllocatorMpcPMEM final : public Allocator
{
	private:
	sctk_pmem_desc_t **_pmems;
	AllocatorStatistics *_stats;
	short *_cpu_2_NUMA;
	int _num_NUMA_nodes;

	public:
	AllocatorMpcPMEM (allocation_functions_t &af);
	~AllocatorMpcPMEM();

	void*  malloc (size_t);
	void*  calloc (size_t,size_t);
	int    posix_memalign (void **, size_t, size_t);
	void   free (void *);
	void*  realloc (void *, size_t);
	size_t malloc_usable_size (void*);

	void   configure (const char *);
	const char * name (void) const;
	const char * description (void) const;
	void show_statistics (void) const;

	void *memcpy (void *dest, const void *src, size_t n)
	  {
#if defined(PMDK_SUPPORTED)
	    return ::pmem_memcpy (dest, src, n, 0);
#else
	    return ::memcpy (dest, src, n);
#endif
	  }

	bool fits (size_t s) const;
	size_t hwm (void) const;
	void record_unfitted_malloc (size_t s);
	void record_unfitted_calloc (size_t s);
	void record_unfitted_aligned_malloc (size_t s);
	void record_unfitted_realloc (size_t s);

	void record_source_realloc (size_t s);
	void record_target_realloc (size_t s);
	void record_self_realloc (size_t s);

	void record_realloc_forward_malloc (void);
};
