// Copyright (c) 2012, Cornell University
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
#include "daemon/replication_manager_pending.h"

using hyperdex::replication_manager;

replication_manager :: pending :: pending(std::auto_ptr<e::buffer> _backing,
                                          const region_id& _reg_id,
                                          uint64_t _seq_id,
                                          bool _fresh,
                                          bool _has_value,
                                          const std::vector<e::slice>& _value,
                                          server_id _client, uint64_t _nonce,
                                          uint64_t _recv_config_version,
                                          const virtual_server_id& _recv)
    : backing(_backing)
    , reg_id(_reg_id)
    , seq_id(_seq_id)
    , has_value(_has_value)
    , value(_value)
    , recv_config_version(_recv_config_version)
    , recv(_recv)
    , sent_config_version(0)
    , sent()
    , fresh(_fresh)
    , acked(false)
    , client(_client)
    , nonce(_nonce)
    , old_hashes()
    , new_hashes()
    , this_old_region()
    , this_new_region()
    , prev_region()
    , next_region()
    , m_ref(0)
{
}

replication_manager :: pending :: ~pending() throw ()
{
}
