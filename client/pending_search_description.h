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

#ifndef hyperdex_client_pending_search_description_h_
#define hyperdex_client_pending_search_description_h_

// STL
#ifdef _MSC_VER
#include <memory>
#else
#include <tr1/memory>
#endif

// HyperDex
#include "client/pending.h"
#include "client/refcount.h"

class hyperclient::pending_search_description : public hyperclient::pending
{
    public:
        pending_search_description(int64_t searchid,
                                   hyperclient_returncode* status,
                                   e::intrusive_ptr<description> sd);
        virtual ~pending_search_description() throw ();

    public:
        virtual hyperdex::network_msgtype request_type();
        virtual int64_t handle_response(hyperclient* cl,
                                        const server_id& id,
                                        std::auto_ptr<e::buffer> msg,
                                        hyperdex::network_msgtype type,
                                        hyperclient_returncode* status);

    private:
        pending_search_description(const pending_search_description& other);

    private:
        pending_search_description& operator = (const pending_search_description& rhs);

    private:
        int64_t m_searchid;
        e::intrusive_ptr<description> m_sd;
};

#endif // hyperdex_client_pending_search_description_h_
