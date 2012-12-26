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
#include "coordinator/coordinator.h"
#include "coordinator/meta.h"

using hyperdex::coordinator;

extern "C"
{

void*
hyperdex_coordinator_create(struct replicant_state_machine_context* ctx)
{
    if (replicant_state_machine_condition_create(ctx, "config") < 0)
    {
        replicant_state_machine_log_error(ctx, "condition creation failed");
        return NULL;
    }

    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        replicant_state_machine_log_error(ctx, "memory allocation failed");
    }

    c->regenerate(ctx);
    return c;
}

void*
hyperdex_coordinator_recreate(struct replicant_state_machine_context* ctx,
                              const char* /*data*/, size_t /*data_sz*/)
{
    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        replicant_state_machine_log_error(ctx, "memory allocation failed");
    }

    replicant_state_machine_log_error(ctx, "recreate is not implemented");
    // XXX recreate from (data,data_sz)
    c->regenerate(ctx);
    return c;
}

void
hyperdex_coordinator_destroy(struct replicant_state_machine_context*, void* obj)
{
    if (obj)
    {
        delete static_cast<coordinator*>(obj);
    }
}

void
hyperdex_coordinator_snapshot(struct replicant_state_machine_context* ctx,
                              void* /*obj*/, const char** data, size_t* sz)
{
    replicant_state_machine_log_error(ctx, "snapshot is not implemented");
    // XXX snapshot to (data,data_sz)
    *data = NULL;
    *sz = 0;
}

} // extern "C"
