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

namespace hyperdex
{

// HyperDisk returncode occupies [8320, 8448)
enum network_returncode
{
    NET_SUCCESS     = 8320,
    NET_NOTFOUND    = 8321,
    NET_WRONGARITY  = 8322,
    NET_NOTUS       = 8323,
    NET_SERVERERROR = 8324
};

enum network_msgtype
{
    REQ_GET         = 8,
    RESP_GET        = 9,

    REQ_PUT         = 10,
    RESP_PUT        = 11,

    REQ_DEL         = 12,
    RESP_DEL        = 13,

    REQ_UPDATE      = 14,
    RESP_UPDATE     = 15,

    REQ_SEARCH_START    = 32,
    REQ_SEARCH_NEXT     = 33,
    REQ_SEARCH_STOP     = 34,
    RESP_SEARCH_ITEM    = 35,
    RESP_SEARCH_DONE    = 36,

    CHAIN_PUT       = 64,
    CHAIN_DEL       = 65,
    CHAIN_PENDING   = 66,
    CHAIN_SUBSPACE  = 67,
    CHAIN_ACK       = 68,

    XFER_MORE       = 96,
    XFER_DATA       = 97,
    XFER_DONE       = 98,

    PACKET_NOP      = 255
};

} // namespace hyperdex

#endif // hyperdex_network_constants_h_
