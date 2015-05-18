/* Copyright (c) 2012-2013, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Replicant nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
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

/* Replicant */
#include <rsm.h>

/* HyperDex */
#include "visibility.h"
#include "coordinator/transitions.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-pedantic"

struct state_machine HYPERDEX_API rsm = {
    hyperdex_coordinator_create,
    hyperdex_coordinator_recreate,
    hyperdex_coordinator_snapshot,
    {{"config_get", hyperdex_coordinator_config_get},
     {"config_ack", hyperdex_coordinator_config_ack},
     {"config_stable", hyperdex_coordinator_config_stable},
     {"server_register", hyperdex_coordinator_server_register},
     {"server_online", hyperdex_coordinator_server_online},
     {"server_offline", hyperdex_coordinator_server_offline},
     {"server_shutdown", hyperdex_coordinator_server_shutdown},
     {"server_kill", hyperdex_coordinator_server_kill},
     {"server_forget", hyperdex_coordinator_server_forget},
     {"server_suspect", hyperdex_coordinator_server_suspect},
     {"report_disconnect", hyperdex_coordinator_report_disconnect},
     {"space_add", hyperdex_coordinator_space_add},
     {"space_rm", hyperdex_coordinator_space_rm},
     {"space_mv", hyperdex_coordinator_space_mv},
     {"index_add", hyperdex_coordinator_index_add},
     {"index_rm", hyperdex_coordinator_index_rm},
     {"transfer_go_live", hyperdex_coordinator_transfer_go_live},
     {"transfer_complete", hyperdex_coordinator_transfer_complete},
     {"checkpoint_stable", hyperdex_coordinator_checkpoint_stable},
     {"periodic", hyperdex_coordinator_periodic},
     {"read_only", hyperdex_coordinator_read_only},
     {"fault_tolerance", hyperdex_coordinator_fault_tolerance},
     {"checkpoints", hyperdex_coordinator_checkpoints},
     {"debug_dump", hyperdex_coordinator_debug_dump},
     {"init", hyperdex_coordinator_init},
     {NULL, NULL}}
};

#pragma GCC diagnostic pop
