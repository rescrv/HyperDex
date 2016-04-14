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

#ifndef hyperdex_daemon_index_map_h_
#define hyperdex_daemon_index_map_h_

// HyperDex
#include "namespace.h"
#include "common/datatype_info.h"
#include "daemon/index_container.h"

BEGIN_HYPERDEX_NAMESPACE

class index_map : public index_container
{
public:
	index_map(hyperdatatype key_datatype,
	          hyperdatatype val_datatype);
	virtual ~index_map() throw ();

private:
	virtual hyperdatatype datatype() const;
	virtual void extract_elements(const e::slice &container,
	                              std::vector<e::slice> *elems) const;
	virtual const datatype_info *element_datatype_info() const;
	virtual const index_info *element_index_info() const;

private:
	hyperdatatype m_key_datatype;
	hyperdatatype m_val_datatype;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_index_map_h_
