// Copyright (c) 2013, Cornell University
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

#define __STDC_LIMIT_MACROS

#ifndef hyperdex_admin_coord_rpc_h_
#define hyperdex_admin_coord_rpc_h_

// STL
#include <memory>

// e
#include <e/intrusive_ptr.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "include/hyperdex/admin.h"
#include "namespace.h"
#include "common/configuration.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "admin/yieldable.h"

BEGIN_HYPERDEX_NAMESPACE
class admin;

class coord_rpc : public yieldable
{
    public:
        coord_rpc(uint64_t admin_visible_id,
                  hyperdex_admin_returncode* status);
        virtual ~coord_rpc() throw ();

    public:
        virtual bool handle_response(admin* adm,
                                     hyperdex_admin_returncode* status) = 0;

    public:
        replicant_returncode repl_status;
        char* repl_output;
        size_t repl_output_sz;

    protected:
        friend class e::intrusive_ptr<coord_rpc>;

    private:
        coord_rpc(const coord_rpc&);
        coord_rpc& operator = (const coord_rpc&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_coord_rpc_h_
