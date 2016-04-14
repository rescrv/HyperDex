// Copyright (c) 2011-2012, Cornell University
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

#ifndef hyperdex_client_constants_h_
#define hyperdex_client_constants_h_

// BusyBee
#include <busybee_constants.h>

#define HYPERDEX_CLIENT_HEADER_SIZE_REQ (BUSYBEE_HEADER_SIZE \
                                         + sizeof(uint8_t) /*mt*/ \
                                         + sizeof(uint8_t) /*flags*/ \
                                         + sizeof(uint64_t) /*version*/ \
                                         + sizeof(uint64_t) /*vidt*/ \
                                         + sizeof(uint64_t) /*nonce*/)
#define HYPERDEX_CLIENT_HEADER_SIZE_RESP (BUSYBEE_HEADER_SIZE \
                                          + sizeof(uint8_t) /*mt*/ \
                                          + sizeof(uint64_t) /*vidt*/ \
                                          + sizeof(uint64_t) /*nonce*/)

#endif // hyperdex_client_constants_h_
