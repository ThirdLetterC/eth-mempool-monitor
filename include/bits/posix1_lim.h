#ifndef ETH_MEMPOOL_MONITOR_POSIX1_LIM_H
#define ETH_MEMPOOL_MONITOR_POSIX1_LIM_H

#if defined(__GLIBC__)
#include_next <bits/posix1_lim.h>
#else
#include <limits.h>
#endif

#endif
