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

// HyperDaemon
#include "hyperdaemon/runtimeconfig.h"

e::envconfig<unsigned int> hyperdaemon::PREALLOCATIONS_PER_SECOND("HYPERDEX_PREALLOCATIONS_PER_SECOND", 2);
e::envconfig<unsigned int> hyperdaemon::OPTIMISM_BURSTS_PER_SECOND("HYPERDEX_OPTIMISM_BURSTS_PER_SECOND", 2);
e::envconfig<unsigned int> hyperdaemon::FLUSH_THREADS("HYPERDEX_FLUSH_THREADS", 4);
e::envconfig<size_t> hyperdaemon::LOCK_STRIPING("HYPERDEX_LOCK_STRIPING", 1024);
e::envconfig<size_t> hyperdaemon::TRANSFERS_IN_FLIGHT("HYPERDEX_TRANSFERS_IN_FLIGHT", 8);
e::envconfig<uint16_t> hyperdaemon::REPLICATION_HASHTABLE_SIZE("HYPERDEX_REPLICATION_HASHTABLE_SIZE", 10);
e::envconfig<uint16_t> hyperdaemon::STATE_TRANSFER_HASHTABLE_SIZE("HYPERDEX_STATE_TRANSFER_HASHTABLE_SIZE", 10);
