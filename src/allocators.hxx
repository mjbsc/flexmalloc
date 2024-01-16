// Author: Harald Servat <harald.servat@intel.com>
// Date: Feb 10, 2017
// License: To determine

#pragma once

#include <stdlib.h>

#include "allocator-statistics.hxx"
#include "allocator.hxx"

#if defined(MEMKIND_SUPPORTED)
# define NUM_MEMKIND_ALLOCATORS 2
#else
# define NUM_MEMKIND_ALLOCATORS 0
#endif

#if defined(MPC_SUPPORTED)
# define NUM_MPC_ALLOCATORS 1
#else
# define NUM_MPC_ALLOCATORS 0
#endif

#define NUM_ALLOCATORS	(1 + NUM_MEMKIND_ALLOCATORS + NUM_MPC_ALLOCATORS)

class Allocators
{
	private:
	Allocator * allocators[NUM_ALLOCATORS+1]; // +1 for null-terminated

	public:
	Allocators (allocation_functions_t &, const char * definitions);
	~Allocators ();
	Allocator * get (const char *name);
	Allocator ** get (void);
	void show_statistics (void) const;
};
