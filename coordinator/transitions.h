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
 *     * Neither the name of NyperDex nor the names of its contributors may be
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

#ifndef hyperdex_coordinator_transitions_h_
#define hyperdex_coordinator_transitions_h_
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* Replicant */
#include <rsm.h>

void*
hyperdex_coordinator_create(struct rsm_context* ctx);

void*
hyperdex_coordinator_recreate(struct rsm_context* ctx,
                              const char* data, size_t data_sz);

int
hyperdex_coordinator_snapshot(struct rsm_context* ctx,
                              void* obj, char** data, size_t* sz);

#define TRANSITION(X) void \
    hyperdex_coordinator_ ## X(struct rsm_context* ctx, \
                               void* obj, const char* data, size_t data_sz)

TRANSITION(init);

TRANSITION(read_only);
TRANSITION(fault_tolerance);

TRANSITION(config_get);
TRANSITION(config_ack);
TRANSITION(config_stable);

TRANSITION(server_register);
TRANSITION(server_online);
TRANSITION(server_offline);
TRANSITION(server_shutdown);
TRANSITION(server_kill);
TRANSITION(server_forget);
TRANSITION(server_suspect);
TRANSITION(report_disconnect);

TRANSITION(space_add);
TRANSITION(space_rm);
TRANSITION(space_mv);

TRANSITION(index_add);
TRANSITION(index_rm);

TRANSITION(transfer_go_live);
TRANSITION(transfer_complete);

TRANSITION(checkpoint_stable);
TRANSITION(checkpoints);

TRANSITION(periodic);

TRANSITION(debug_dump);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* hyperdex_coordinator_transitions_h_ */
