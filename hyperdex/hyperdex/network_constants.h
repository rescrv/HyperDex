// Copyright (c) 2011, Cornell University
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

#ifndef hyperdex_network_constants_h_
#define hyperdex_network_constants_h_

// C++
#include <iostream>

// HyperDex
#include "common/macros.h"

namespace hyperdex
{

// HyperDisk returncode occupies [8320, 8448)
enum network_returncode
{
    NET_SUCCESS     = 8320,
    NET_NOTFOUND    = 8321,
    NET_BADDIMSPEC  = 8322,
    NET_NOTUS       = 8323,
    NET_SERVERERROR = 8324,
    NET_CMPFAIL     = 8325,
    NET_BADMICROS   = 8326,
    NET_READONLY    = 8327,
    NET_OVERFLOW    = 8328
};

enum network_msgtype
{
    REQ_GET         = 8,
    RESP_GET        = 9,

    REQ_ATOMIC      = 16,
    RESP_ATOMIC     = 17,

    REQ_SEARCH_START    = 32,
    REQ_SEARCH_NEXT     = 33,
    REQ_SEARCH_STOP     = 34,
    RESP_SEARCH_ITEM    = 35,
    RESP_SEARCH_DONE    = 36,

    REQ_SORTED_SEARCH   = 40,
    RESP_SORTED_SEARCH  = 41,

    REQ_GROUP_DEL   = 48,
    RESP_GROUP_DEL  = 49,

    REQ_COUNT       = 50,
    RESP_COUNT      = 51,

    CHAIN_PUT       = 64,
    CHAIN_DEL       = 65,
    CHAIN_SUBSPACE  = 67,
    CHAIN_ACK       = 68,

    CONFIGMISMATCH  = 254,
    PACKET_NOP      = 255
};

inline std::ostream&
operator << (std::ostream& lhs, const network_msgtype& rhs)
{
    switch(rhs)
    {
        stringify(REQ_GET);
        stringify(RESP_GET);
        stringify(REQ_ATOMIC);
        stringify(RESP_ATOMIC);
        stringify(REQ_SEARCH_START);
        stringify(REQ_SEARCH_NEXT);
        stringify(REQ_SEARCH_STOP);
        stringify(RESP_SEARCH_ITEM);
        stringify(RESP_SEARCH_DONE);
        stringify(REQ_SORTED_SEARCH);
        stringify(RESP_SORTED_SEARCH);
        stringify(REQ_GROUP_DEL);
        stringify(RESP_GROUP_DEL);
        stringify(REQ_COUNT);
        stringify(RESP_COUNT);
        stringify(CHAIN_PUT);
        stringify(CHAIN_DEL);
        stringify(CHAIN_SUBSPACE);
        stringify(CHAIN_ACK);
        stringify(CONFIGMISMATCH);
        stringify(PACKET_NOP);
        default:
            lhs << "unknown network_msgtype";
            break;
    }

    return lhs;
}

} // namespace hyperdex

#endif // hyperdex_network_constants_h_
