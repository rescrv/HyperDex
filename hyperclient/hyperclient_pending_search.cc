// Copyright (c) 2011-2012, Cornell University
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

// HyperClient
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending_search.h"
#include "hyperclient/util.h"

hyperclient :: pending_search :: pending_search(int64_t searchid,
                                                std::tr1::shared_ptr<uint64_t> refcount,
                                                hyperclient_returncode* status,
                                                hyperclient_attribute** attrs,
                                                size_t* attrs_sz)
    : pending(status)
    , m_searchid(searchid)
    , m_reqtype(hyperdex::REQ_SEARCH_START)
    , m_refcount(refcount)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
    ++*m_refcount;
    this->set_client_visible_id(searchid);
}

hyperclient :: pending_search :: ~pending_search() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_search :: request_type()
{
    return m_reqtype;
}

int64_t
hyperclient :: pending_search :: handle_response(hyperclient* cl,
                                                 const po6::net::location& sender,
                                                 std::auto_ptr<e::buffer> msg,
                                                 hyperdex::network_msgtype type,
                                                 hyperclient_returncode* status)
{
    assert(*m_refcount > 0);
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_SEARCH_ITEM && type != hyperdex::RESP_SEARCH_DONE)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    // If it is a SEARCH_DONE message.
    if (type == hyperdex::RESP_SEARCH_DONE)
    {
        if (--*m_refcount == 0)
        {
            set_status(HYPERCLIENT_SEARCHDONE);
            return client_visible_id();
        }

        // Silently remove this operation
        return 0;
    }

    // Otheriwise it is a SEARCH_ITEM message.
    e::slice key;
    std::vector<e::slice> value;

    if ((msg->unpack_from(HYPERCLIENT_HEADER_SIZE) >> key >> value).error())
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);

        if (--*m_refcount == 0)
        {
            cl->m_complete.push(completedop(this, HYPERCLIENT_SEARCHDONE, 0));
        }

        return 0;
    }

    hyperclient_returncode op_status;

    if (!value_to_attributes(*cl->m_config, this->entity(), key.data(), key.size(),
                             value, status, &op_status, m_attrs, m_attrs_sz))
    {
        set_status(op_status);

        if (--*m_refcount == 0)
        {
            cl->m_complete.push(completedop(this, HYPERCLIENT_SEARCHDONE, 0));
        }

        return client_visible_id();
    }

    e::guard g = e::makeguard(hyperclient_destroy_attrs, *m_attrs, *m_attrs_sz);
    std::auto_ptr<e::buffer> smsg(e::buffer::create(HYPERCLIENT_HEADER_SIZE + sizeof(uint64_t)));
    bool packed = !(smsg->pack_at(HYPERCLIENT_HEADER_SIZE) << static_cast<uint64_t>(m_searchid)).error();
    assert(packed);

    set_server_visible_nonce(cl->m_server_nonce);
    ++cl->m_server_nonce;
    m_reqtype = hyperdex::REQ_SEARCH_NEXT;

    if (cl->send(this, smsg) < 0)
    {
        cl->killall(sender, HYPERCLIENT_RECONFIGURE);

        if (--*m_refcount == 0)
        {
            cl->m_complete.push(completedop(this, HYPERCLIENT_SEARCHDONE, 0));
        }

        return 0;
    }

    cl->m_incomplete.insert(std::make_pair(server_visible_nonce(), this));
    set_status(HYPERCLIENT_SUCCESS);
    g.dismiss();
    return client_visible_id();
}
