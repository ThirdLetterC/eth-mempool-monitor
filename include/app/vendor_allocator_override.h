#ifndef APP_VENDOR_ALLOCATOR_OVERRIDE_H
#define APP_VENDOR_ALLOCATOR_OVERRIDE_H

/*
 * Allocator boundary for bundled vendor sources without native hooks.
 *
 * This header is force-included only for those translation units. Including
 * the system declarations before defining the aliases prevents the aliases
 * from rewriting libc prototypes.
 */
#include <stdlib.h> // IWYU pragma: keep
#include <string.h> // IWYU pragma: keep

#if defined(USE_MIMALLOC)
#include <mimalloc.h>

#define malloc mi_malloc
#define calloc mi_calloc
#define realloc mi_realloc
#define free mi_free
#define strdup mi_strdup
#endif

#endif
