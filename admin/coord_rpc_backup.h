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

#ifndef hyperdex_admin_coord_rpc_backup_h_
#define hyperdex_admin_coord_rpc_backup_h_

// HyperDex
#include "admin/coord_rpc.h"

BEGIN_HYPERDEX_NAMESPACE

class coord_rpc_backup : public coord_rpc
{
public:
	coord_rpc_backup(uint64_t admin_visible_id,
	                 hyperdex_admin_returncode *status,
	                 const char *path);
	virtual ~coord_rpc_backup() throw ();

public:
	virtual bool can_yield();
	virtual bool yield(hyperdex_admin_returncode *status);

public:
	virtual bool handle_response(admin *adm,
	                             hyperdex_admin_returncode *status);

private:
	coord_rpc_backup(const coord_rpc_backup &);
	coord_rpc_backup &operator = (const coord_rpc_backup &);

private:
	std::string m_path;
	bool m_done;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_coord_rpc_generic_h_
