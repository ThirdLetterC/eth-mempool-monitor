/*
 * Copyright (c) 2020, Michael Grunder <michael dot grunder at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <string.h>

#if defined(HIREDIS_USE_MIMALLOC)
#include <mimalloc.h>
#endif

#include "hiredis/alloc.h"

#if defined(HIREDIS_USE_MIMALLOC)
#define HIREDIS_MALLOC_IMPL mi_malloc
#define HIREDIS_CALLOC_IMPL mi_calloc
#define HIREDIS_REALLOC_IMPL mi_realloc
#define HIREDIS_STRDUP_IMPL mi_strdup
#define HIREDIS_FREE_IMPL mi_free
#else
#define HIREDIS_MALLOC_IMPL malloc
#define HIREDIS_CALLOC_IMPL calloc
#define HIREDIS_REALLOC_IMPL realloc
#define HIREDIS_STRDUP_IMPL strdup
#define HIREDIS_FREE_IMPL free
#endif

hiredisAllocFuncs hiredisAllocFns = {
    .mallocFn = HIREDIS_MALLOC_IMPL,
    .callocFn = HIREDIS_CALLOC_IMPL,
    .reallocFn = HIREDIS_REALLOC_IMPL,
    .strdupFn = HIREDIS_STRDUP_IMPL,
    .freeFn = HIREDIS_FREE_IMPL,
};

/* Override hiredis' allocators with ones supplied by the user */
hiredisAllocFuncs hiredisSetAllocators(hiredisAllocFuncs *override) {
  hiredisAllocFuncs orig = hiredisAllocFns;

  hiredisAllocFns = *override;

  return orig;
}

/* Reset allocators to use libc defaults */
void hiredisResetAllocators() {
  hiredisAllocFns = (hiredisAllocFuncs){
      .mallocFn = HIREDIS_MALLOC_IMPL,
      .callocFn = HIREDIS_CALLOC_IMPL,
      .reallocFn = HIREDIS_REALLOC_IMPL,
      .strdupFn = HIREDIS_STRDUP_IMPL,
      .freeFn = HIREDIS_FREE_IMPL,
  };
}
