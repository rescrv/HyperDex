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

#ifndef hyperdex_common_network_msgtype_h_
#define hyperdex_common_network_msgtype_h_

// C++
#include <iostream>

// HyperDex
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE

enum network_msgtype
{
    REQ_GET         = 8,
    RESP_GET        = 9,

    REQ_GET_PARTIAL = 10,
    RESP_GET_PARTIAL = 11,

    REQ_ATOMIC      = 16,
    RESP_ATOMIC     = 17,

    REQ_SEARCH_START    = 32,
    REQ_SEARCH_NEXT     = 33,
    REQ_SEARCH_STOP     = 34,
    RESP_SEARCH_ITEM    = 35,
    RESP_SEARCH_DONE    = 36,

    REQ_SORTED_SEARCH   = 40,
    RESP_SORTED_SEARCH  = 41,

    /* 48, 49 retired */

    REQ_COUNT       = 50,
    RESP_COUNT      = 51,

    REQ_SEARCH_DESCRIBE  = 52,
    RESP_SEARCH_DESCRIBE = 53,

    REQ_GROUP_ATOMIC = 54,
    RESP_GROUP_ATOMIC = 55,

    CHAIN_OP        = 64,
    CHAIN_SUBSPACE  = 65,
    CHAIN_ACK       = 66,
    /* 67 retired */

    XFER_OP  = 80,
    XFER_ACK = 81,
    XFER_HS  = 82, // handshake syn
    XFER_HSA = 83, // handshake syn-ack
    XFER_HA  = 84, // handshake ack
    XFER_HW  = 85, // wiped

    BACKUP = 126,
    PERF_COUNTERS = 127,

    CONFIGMISMATCH  = 254,
    PACKET_NOP      = 255
};

std::ostream&
operator << (std::ostream& lhs, const network_msgtype& rhs);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_network_msgtype_h_
