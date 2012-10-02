// Copyright (c) 2012, Robert Escriva
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef append_only_log_constants_h_
#define append_only_log_constants_h_

// C
#include <stdint.h>

#define ENTRY_HEADER_SIZE (sizeof(uint32_t) /*hash*/ \
                           + sizeof(uint16_t) /*len*/ \
                           + sizeof(uint8_t) /*type*/ \
                           + sizeof(uint16_t) + sizeof(uint32_t) /*id*/)
// Must be a power of 2
#define BLOCK_SIZE 16384
#define BLOCKS_PER_SEGMENT ((BLOCK_SIZE - sizeof(uint32_t) - sizeof(uint64_t)) / 4)
#define SEGMENT_SIZE (BLOCKS_PER_SEGMENT * BLOCK_SIZE + BLOCK_SIZE)

#define MAX_WRITE_SIZE ((BLOCKS_PER_SEGMENT - 1) * (BLOCK_SIZE - ENTRY_HEADER_SIZE))

#define ID_UPPER_BOUND (1ULL << 48ULL)

#define TYPE_FULL 1
#define TYPE_FIRST 2
#define TYPE_MIDDLE 3
#define TYPE_LAST 4
#define TYPE_REMOVED 5

#endif // append_only_log_constants_h_
