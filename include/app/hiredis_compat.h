#pragma once

/*
 * Hiredis is vendored and intentionally not held to first-party conversion
 * diagnostics. Keep the suppression confined to parsing its public headers.
 */
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#pragma clang diagnostic ignored "-Wsign-conversion"
#endif

#include "hiredis/hiredis.h" // IWYU pragma: export

#if defined(__clang__)
#pragma clang diagnostic pop
#endif
