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

// HyperDex
#include "common/macros.h"
#include "common/network_msgtype.h"

std::ostream&
hyperdex :: operator << (std::ostream& lhs, const network_msgtype& rhs)
{
    switch(rhs)
    {
        STRINGIFY(REQ_GET);
        STRINGIFY(RESP_GET);
        STRINGIFY(REQ_GET_PARTIAL);
        STRINGIFY(RESP_GET_PARTIAL);
        STRINGIFY(REQ_ATOMIC);
        STRINGIFY(RESP_ATOMIC);
        STRINGIFY(REQ_SEARCH_START);
        STRINGIFY(REQ_SEARCH_NEXT);
        STRINGIFY(REQ_SEARCH_STOP);
        STRINGIFY(RESP_SEARCH_ITEM);
        STRINGIFY(RESP_SEARCH_DONE);
        STRINGIFY(REQ_SORTED_SEARCH);
        STRINGIFY(RESP_SORTED_SEARCH);
        STRINGIFY(REQ_COUNT);
        STRINGIFY(RESP_COUNT);
        STRINGIFY(REQ_SEARCH_DESCRIBE);
        STRINGIFY(RESP_SEARCH_DESCRIBE);
        STRINGIFY(REQ_GROUP_ATOMIC);
        STRINGIFY(RESP_GROUP_ATOMIC);
        STRINGIFY(CHAIN_OP);
        STRINGIFY(CHAIN_SUBSPACE);
        STRINGIFY(CHAIN_ACK);
        STRINGIFY(XFER_OP);
        STRINGIFY(XFER_ACK);
        STRINGIFY(XFER_HS);
        STRINGIFY(XFER_HSA);
        STRINGIFY(XFER_HA);
        STRINGIFY(XFER_HW);
        STRINGIFY(BACKUP);
        STRINGIFY(PERF_COUNTERS);
        STRINGIFY(CONFIGMISMATCH);
        STRINGIFY(PACKET_NOP);
        default:
            lhs << "unknown network_msgtype";
            break;
    }

    return lhs;
}
