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

#define __STDC_LIMIT_MACROS

// Linux
#include <sys/epoll.h>

// STL
#include <tr1/memory>

// e
#include <e/guard.h>

// HyperDex
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/datatype.h"
#include "hyperdex/hyperdex/instance.h"
#include "hyperdex/hyperdex/network_constants.h"

// HyperClient
#include "hyperclient/hyperclient.h"

#define HDRSIZE (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint8_t) + 2 * sizeof(uint16_t) + 2 * hyperdex::entityid::SERIALIZEDSIZE + sizeof(uint64_t))

// XXX 2012-01-22 When failures happen, this code is not robust.  It may throw
// exceptions, and it will fail to properly cleanup state.  For instance, if a
// socket error is detected when sending a SEARCH_ITEM_NEXT message, it returns
// "FAIL", and then enqueues a SEARCH DONE.  The errors will be delivered to
// clients after the SEARCH_DONE.  This will be more than a simple fix.
// Post-SIGCOMM.

////////////////////////////////// C Wrappers //////////////////////////////////

extern "C"
{

struct hyperclient*
hyperclient_create(const char* coordinator, in_port_t port)
{
    try
    {
        std::auto_ptr<hyperclient> ret(new hyperclient(coordinator, port));
        return ret.release();
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

void
hyperclient_destroy(struct hyperclient* client)
{
    delete client;
}

int64_t
hyperclient_get(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status,
                struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    try
    {
        return client->get(space, key, key_sz, status, attrs, attrs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_put(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, hyperclient_returncode* status)
{
    try
    {
        return client->put(space, key, key_sz, attrs, attrs_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_condput(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, const struct hyperclient_attribute* condattrs,
                size_t condattrs_sz, const struct hyperclient_attribute* attrs,
                size_t attrs_sz, hyperclient_returncode* status)
{
    try
    {
        return client->condput(space, key, key_sz, condattrs, condattrs_sz, attrs, attrs_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_del(struct hyperclient* client, const char* space, const char* key,
                size_t key_sz, hyperclient_returncode* status)
{
    try
    {
        return client->del(space, key, key_sz, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_search(struct hyperclient* client, const char* space,
                   const struct hyperclient_attribute* eq, size_t eq_sz,
                   const struct hyperclient_range_query* rn, size_t rn_sz,
                   enum hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    try
    {
        return client->search(space, eq, eq_sz, rn, rn_sz, status, attrs, attrs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

int64_t
hyperclient_loop(struct hyperclient* client, int timeout, hyperclient_returncode* status)
{
    try
    {
        return client->loop(timeout, status);
    }
    catch (po6::error& e)
    {
        errno = e;
        *status = HYPERCLIENT_SEEERRNO;
        return -1;
    }
    catch (...)
    {
        *status = HYPERCLIENT_EXCEPTION;
        return -1;
    }
}

void
hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t /*attrs_sz*/)
{
    free(attrs);
}

} // extern "C"

/////////////////////////////// Utility Functions //////////////////////////////

static hyperclient_datatype
hd_to_hc_type(hyperdex::datatype in)
{
    switch (in)
    {
        case hyperdex::DATATYPE_STRING:
            return HYPERDATATYPE_STRING;
        case hyperdex::DATATYPE_UINT64:
            return HYPERDATATYPE_UINT64;
        default:
            return HYPERDATATYPE_GARBAGE;
    }
}

static bool
attributes_from_value(const hyperdex::configuration& config,
                      const hyperdex::entityid& entity,
                      const uint8_t* key,
                      size_t key_sz,
                      const std::vector<e::slice>& value,
                      hyperclient_returncode* status,
                      hyperclient_attribute** attrs,
                      size_t* attrs_sz)
{
    std::vector<hyperdex::attribute> dimension_names = config.dimension_names(entity.get_space());

    if (value.size() + 1 != dimension_names.size())
    {
        *status = HYPERCLIENT_SERVERERROR;
        return false;
    }

    size_t sz = sizeof(hyperclient_attribute) * dimension_names.size() + key_sz
              + dimension_names[0].name.size() + 1;

    for (size_t i = 0; i < value.size(); ++i)
    {
        sz += dimension_names[i + 1].name.size() + 1 + value[i].size();
    }

    std::vector<hyperclient_attribute> ha;
    ha.reserve(dimension_names.size());
    char* ret = static_cast<char*>(malloc(sz));

    if (!ret)
    {
        *status = HYPERCLIENT_SEEERRNO;
        return false;
    }

    e::guard g = e::makeguard(free, ret);
    char* data = ret + sizeof(hyperclient_attribute) * value.size();

    if (key)
    {
        data += sizeof(hyperclient_attribute);
        ha.push_back(hyperclient_attribute());
        size_t attr_sz = dimension_names[0].name.size() + 1;
        ha.back().attr = data;
        memmove(data, dimension_names[0].name.c_str(), attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, key, key_sz);
        data += key_sz;
        ha.back().value_sz = key_sz;
        ha.back().datatype = hd_to_hc_type(dimension_names[0].type);
    }

    for (size_t i = 0; i < value.size(); ++i)
    {
        ha.push_back(hyperclient_attribute());
        size_t attr_sz = dimension_names[i + 1].name.size() + 1;
        ha.back().attr = data;
        memmove(data, dimension_names[i + 1].name.c_str(), attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, value[i].data(), value[i].size());
        data += value[i].size();
        ha.back().value_sz = value[i].size();
        ha.back().datatype = hd_to_hc_type(dimension_names[i + 1].type);
    }

    memmove(ret, &ha.front(), sizeof(hyperclient_attribute) * ha.size());
    *status = HYPERCLIENT_SUCCESS;
    *attrs = reinterpret_cast<hyperclient_attribute*>(ret);
    *attrs_sz = ha.size();
    g.dismiss();
    return true;
}


///////////////////////////////// Channel Class ////////////////////////////////

class hyperclient::channel
{
    public:
        channel(const po6::net::location& loc);

    public:
        // The entity by which the remote host knows us
        const hyperdex::entityid& entity() const { return m_ent; }

    public:
        uint64_t generate_nonce() { return m_nonce++; }
        void set_nonce(uint64_t nonce) { m_nonce = nonce; }
        void set_entity(hyperdex::entityid ent) { m_ent = ent; }
        po6::net::location location() { return m_loc; }
        po6::net::socket& sock() { return m_sock; }

    private:
        friend class e::intrusive_ptr<channel>;

    private:
        ~channel() throw ();

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
        uint64_t m_nonce;
        hyperdex::entityid m_ent;
        po6::net::location m_loc;
        po6::net::socket m_sock;
};

hyperclient :: channel :: channel(const po6::net::location& loc)
    : m_ref(0)
    , m_nonce(1)
    , m_ent(hyperdex::configuration::CLIENTSPACE, 0, 0, 0, 0)
    , m_loc(loc)
    , m_sock(loc.address.family(), SOCK_STREAM, IPPROTO_TCP)
{
    m_sock.connect(loc);
    m_sock.set_tcp_nodelay();
}

hyperclient :: channel :: ~channel() throw ()
{
}

//////////////////////////////// Failedop Class ////////////////////////////////

class hyperclient::completedop
{
    public:
        completedop(e::intrusive_ptr<pending> _op, hyperclient_returncode _why)
            : op(_op), why(_why) {}
        completedop(const completedop& other)
            : op(other.op), why(other.why) {}
        ~completedop() throw () {}

    public:
        completedop& operator = (const completedop& rhs)
        { op = rhs.op; why = rhs.why; return *this; }

    public:
        e::intrusive_ptr<pending> op;
        hyperclient_returncode why;
};

///////////////////////////////// Pending Class ////////////////////////////////

enum handled_how
{
    KEEP,
    SILENTREMOVE,
    REMOVE,
    FAIL
};

class hyperclient::pending
{
    public:
        pending(hyperclient_returncode* status);
        virtual ~pending() throw ();

    public:
        int64_t id() const { return m_id; }
        uint64_t nonce() const { return m_nonce; }
        e::intrusive_ptr<channel> chan() const { return m_chan; }
        const hyperdex::entityid& entity() const { return m_ent; }
        const hyperdex::instance& instance() const { return m_inst; }

    public:
        void set_id(int64_t _id) { m_id = _id; }
        void set_nonce(uint64_t _nonce) { m_nonce = _nonce; }
        void set_channel(e::intrusive_ptr<channel> _chan) { m_chan = _chan; }
        void set_entity(hyperdex::entityid ent) { m_ent = ent; }
        void set_instance(hyperdex::instance inst) { m_inst = inst; }
        void set_status(hyperclient_returncode status) { *m_status = status; }

    public:
        virtual hyperdex::network_msgtype request_type() const = 0;
        virtual bool matches_response_type(hyperdex::network_msgtype t) const = 0;
        virtual handled_how handle_response(hyperdex::network_msgtype type,
                                            e::buffer* msg,
                                            hyperclient_returncode* status) = 0;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        pending(const pending& other);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        pending& operator = (const pending& rhs);

    private:
        size_t m_ref;
        e::intrusive_ptr<channel> m_chan;
        int64_t m_id;
        uint64_t m_nonce;
        hyperdex::entityid m_ent;
        hyperdex::instance m_inst;
        hyperclient_returncode* m_status;
        bool m_finished;
};

hyperclient :: pending :: pending(hyperclient_returncode* status)
    : m_ref(0)
    , m_chan()
    , m_id(-1)
    , m_nonce(0)
    , m_ent()
    , m_inst()
    , m_status(status)
    , m_finished(false)
{
}

hyperclient :: pending :: ~pending() throw ()
{
}

class hyperclient::pending_get : public hyperclient::pending
{
    public:
        pending_get(hyperclient* cl,
                    hyperclient_returncode* status,
                    struct hyperclient_attribute** attrs,
                    size_t* attrs_sz);
        virtual ~pending_get() throw ();

    public:
        virtual hyperdex::network_msgtype request_type() const;
        virtual bool matches_response_type(hyperdex::network_msgtype t) const;
        virtual handled_how handle_response(hyperdex::network_msgtype type,
                                            e::buffer* msg,
                                            hyperclient_returncode* status);

    private:
        pending_get(const pending_get& other);

    private:
        pending_get& operator = (const pending_get& rhs);

    private:
        hyperclient* m_cl;
        hyperclient_attribute** m_attrs;
        size_t* m_attrs_sz;
};

hyperclient :: pending_get :: pending_get(hyperclient* cl,
                                          hyperclient_returncode* status,
                                          struct hyperclient_attribute** attrs,
                                          size_t* attrs_sz)
    : pending(status)
    , m_cl(cl)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
}

hyperclient :: pending_get :: ~pending_get() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_get :: request_type() const
{
    return hyperdex::REQ_GET;
}

bool
hyperclient :: pending_get :: matches_response_type(hyperdex::network_msgtype t) const
{
    return t == hyperdex::RESP_GET;
}

handled_how
hyperclient :: pending_get :: handle_response(hyperdex::network_msgtype type,
                                              e::buffer* msg,
                                              hyperclient_returncode* status)
{
    assert(matches_response_type(type));

    e::buffer::unpacker up = msg->unpack_from(HDRSIZE);
    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        return REMOVE;
    }

    switch (static_cast<hyperdex::network_returncode>(response))
    {
        case hyperdex::NET_SUCCESS:
            break;
        case hyperdex::NET_NOTFOUND:
            set_status(HYPERCLIENT_NOTFOUND);
            return REMOVE;
        case hyperdex::NET_BADDIMSPEC:
            set_status(HYPERCLIENT_LOGICERROR);
            return REMOVE;
        case hyperdex::NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            return REMOVE;
        case hyperdex::NET_SERVERERROR:
        case hyperdex::NET_CMPFAIL:
        default:
            set_status(HYPERCLIENT_SERVERERROR);
            return REMOVE;
    }

    std::vector<e::slice> value;
    up = up >> value;

    if (up.error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        return REMOVE;
    }

    if (!attributes_from_value(*m_cl->m_config, entity(), NULL, 0, value,
                               status, m_attrs, m_attrs_sz))
    {
        set_status(*status);
        return REMOVE;
    }

    set_status(HYPERCLIENT_SUCCESS);
    return REMOVE;
}

class hyperclient::pending_statusonly : public hyperclient::pending
{
    public:
        pending_statusonly(hyperdex::network_msgtype reqtype,
                           hyperdex::network_msgtype resptype,
                           hyperclient_returncode* status);
        virtual ~pending_statusonly() throw ();

    public:
        virtual hyperdex::network_msgtype request_type() const;
        virtual bool matches_response_type(hyperdex::network_msgtype t) const;
        virtual handled_how handle_response(hyperdex::network_msgtype type,
                                            e::buffer* msg,
                                            hyperclient_returncode* status);

    private:
        pending_statusonly(const pending_statusonly& other);

    private:
        pending_statusonly& operator = (const pending_statusonly& rhs);

    private:
        hyperdex::network_msgtype m_reqtype;
        hyperdex::network_msgtype m_resptype;
};

hyperclient :: pending_statusonly :: pending_statusonly(
                            hyperdex::network_msgtype reqtype,
                            hyperdex::network_msgtype resptype,
                            hyperclient_returncode* status)
    : pending(status)
    , m_reqtype(reqtype)
    , m_resptype(resptype)
{
}

hyperclient :: pending_statusonly :: ~pending_statusonly() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_statusonly :: request_type() const
{
    return m_reqtype;
}

bool
hyperclient :: pending_statusonly :: matches_response_type(hyperdex::network_msgtype t) const
{
    return m_resptype == t;
}

handled_how
hyperclient :: pending_statusonly :: handle_response(hyperdex::network_msgtype type,
                                                     e::buffer* msg,
                                                     hyperclient_returncode*)
{
    assert(matches_response_type(type));

    e::buffer::unpacker up = msg->unpack_from(HDRSIZE);
    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        return REMOVE;
    }

    switch (static_cast<hyperdex::network_returncode>(response))
    {
        case hyperdex::NET_SUCCESS:
            set_status(HYPERCLIENT_SUCCESS);
            break;
        case hyperdex::NET_NOTFOUND:
            set_status(HYPERCLIENT_NOTFOUND);
            break;
        case hyperdex::NET_BADDIMSPEC:
            set_status(HYPERCLIENT_LOGICERROR);
            break;
        case hyperdex::NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            break;
        case hyperdex::NET_CMPFAIL:
            set_status(HYPERCLIENT_CMPFAIL);
            break;
        case hyperdex::NET_SERVERERROR:
        default:
            set_status(HYPERCLIENT_SERVERERROR);
            break;
    }

    return REMOVE;
}

class hyperclient::pending_search : public hyperclient::pending
{
    public:
        pending_search(hyperclient* cl,
                       uint64_t searchid,
                       std::tr1::shared_ptr<uint64_t> refcount,
                       hyperclient_returncode* status,
                       hyperclient_attribute** attrs,
                       size_t* attrs_sz);
        virtual ~pending_search() throw ();

    public:
        virtual hyperdex::network_msgtype request_type() const;
        virtual bool matches_response_type(hyperdex::network_msgtype t) const;
        virtual handled_how handle_response(hyperdex::network_msgtype type,
                                            e::buffer* msg,
                                            hyperclient_returncode* status);

    private:
        pending_search(const pending_search& other);

    private:
        pending_search& operator = (const pending_search& rhs);

    private:
        hyperclient* m_cl;
        hyperdex::network_msgtype m_reqtype;
        uint64_t m_searchid;
        std::tr1::shared_ptr<uint64_t> m_refcount;
        hyperclient_attribute** m_attrs;
        size_t* m_attrs_sz;
};

hyperclient :: pending_search :: pending_search(hyperclient* cl,
                                                uint64_t searchid,
                                                std::tr1::shared_ptr<uint64_t> refcount,
                                                hyperclient_returncode* status,
                                                hyperclient_attribute** attrs,
                                                size_t* attrs_sz)
    : pending(status)
    , m_cl(cl)
    , m_reqtype(hyperdex::REQ_SEARCH_START)
    , m_searchid(searchid)
    , m_refcount(refcount)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
    ++*m_refcount;
}

hyperclient :: pending_search :: ~pending_search() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_search :: request_type() const
{
    return m_reqtype;
}

bool
hyperclient :: pending_search :: matches_response_type(hyperdex::network_msgtype t) const
{
    return t == hyperdex::RESP_SEARCH_ITEM || t == hyperdex::RESP_SEARCH_DONE;
}

handled_how
hyperclient :: pending_search :: handle_response(hyperdex::network_msgtype type,
                                                 e::buffer* msg,
                                                 hyperclient_returncode* status)
{
    assert(matches_response_type(type));
    assert(*m_refcount > 0);

    // If it is a SEARCH_DONE message.
    if (type == hyperdex::RESP_SEARCH_DONE)
    {
        if (--*m_refcount == 0)
        {
            set_status(HYPERCLIENT_SEARCHDONE);
            return REMOVE;
        }

        return SILENTREMOVE;
    }

    // Otheriwise it is a SEARCH_ITEM message.
    e::slice key;
    std::vector<e::slice> value;

    if ((msg->unpack_from(HDRSIZE) >> key >> value).error())
    {
        set_status(HYPERCLIENT_SERVERERROR);

        if (--*m_refcount == 0)
        {
            m_cl->m_completed.push(completedop(this, HYPERCLIENT_SEARCHDONE));
        }

        return REMOVE;
    }

    hyperclient_attribute* attrs;
    size_t attrs_sz;

    if (!attributes_from_value(*m_cl->m_config, entity(),
                               key.data(), key.size(), value,
                               status, &attrs, &attrs_sz))
    {
        set_status(*status);

        if (--*m_refcount == 0)
        {
            m_cl->m_completed.push(completedop(this, HYPERCLIENT_SEARCHDONE));
        }

        return REMOVE;
    }

    e::guard g = e::makeguard(hyperclient_destroy_attrs, attrs, attrs_sz);
    g.dismiss();

    std::auto_ptr<e::buffer> smsg(e::buffer::create(HDRSIZE + sizeof(uint64_t)));
    bool packed = !(smsg->pack_at(HDRSIZE) << static_cast<uint64_t>(id())).error();
    assert(packed);
    uint64_t old_nonce = nonce();
    set_nonce(chan()->generate_nonce());
    m_reqtype = hyperdex::REQ_SEARCH_NEXT;

    if (m_cl->send(chan(), this, smsg.get()) < 0)
    {
        m_cl->killall(chan()->sock().get(), HYPERCLIENT_DISCONNECT);

        if (--*m_refcount == 0)
        {
            m_cl->m_completed.push(completedop(this, HYPERCLIENT_SEARCHDONE));
        }

        return FAIL;
    }

    m_cl->m_requests.erase(std::make_pair(chan()->sock().get(), old_nonce));
    m_cl->m_requests.insert(std::make_pair(std::make_pair(chan()->sock().get(), nonce()), this));
    set_status(HYPERCLIENT_SUCCESS);
    *m_attrs = attrs;
    *m_attrs_sz = attrs_sz;
    g.dismiss();
    return KEEP;
}

///////////////////////////////// Public Class /////////////////////////////////

hyperclient :: hyperclient(const char* coordinator, in_port_t port)
    : m_coord(new hyperdex::coordinatorlink(po6::net::location(coordinator, port)))
    , m_config(new hyperdex::configuration())
    , m_epfd()
    , m_fds(sysconf(_SC_OPEN_MAX), e::intrusive_ptr<channel>())
    , m_instances()
    , m_requests()
    , m_completed()
    , m_requestid(1)
    , m_have_seen_config(false)
{
    m_coord->set_announce("client");
    m_epfd = epoll_create(16384);

    if (m_epfd.get() < 0)
    {
        throw po6::error(errno);
    }
}

hyperclient :: ~hyperclient() throw ()
{
}

int64_t
hyperclient :: get(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_get(this, status, attrs, attrs_sz);
    size_t sz = HDRSIZE
              + sizeof(uint32_t)
              + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HDRSIZE);
    p = p << e::slice(key, key_sz);
    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: put(const char* space, const char* key, size_t key_sz,
                   const struct hyperclient_attribute* attrs, size_t attrs_sz,
                   hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_PUT, hyperdex::RESP_PUT, status);
    size_t sz = HDRSIZE
              + sizeof(uint32_t)
              + key_sz
              + pack_attrs_sz(attrs, attrs_sz);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HDRSIZE);
    p = p << e::slice(key, key_sz);

    int64_t ret = pack_attrs(space, &p, attrs, attrs_sz, status);

    if (ret < 0)
    {
        return ret;
    }

    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: condput(const char* space, const char* key, size_t key_sz,
		       const struct hyperclient_attribute* condattrs, size_t condattrs_sz,
		       const struct hyperclient_attribute* attrs, size_t attrs_sz,
		       hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_CONDPUT, hyperdex::RESP_CONDPUT, status);
    size_t sz = HDRSIZE
              + sizeof(uint32_t)
              + key_sz
              + pack_attrs_sz(condattrs, condattrs_sz)
              + pack_attrs_sz(attrs, attrs_sz);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HDRSIZE);
    p = p << e::slice(key, key_sz);

    int64_t ret = pack_attrs(space, &p, condattrs, condattrs_sz, status);

    if (ret < 0)
    {
        return ret;
    }

    ret = pack_attrs(space, &p, attrs, attrs_sz, status);

    if (ret < 0)
    {
      return ret - condattrs_sz;
    }

    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: del(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending> op;
    op = new pending_statusonly(hyperdex::REQ_DEL, hyperdex::RESP_DEL, status);
    size_t sz = HDRSIZE
              + sizeof(uint32_t)
              + key_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer p = msg->pack_at(HDRSIZE);
    p = p << e::slice(key, key_sz);
    assert(!p.error());
    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: search(const char* space,
                      const struct hyperclient_attribute* eq, size_t eq_sz,
                      const struct hyperclient_range_query* rn, size_t rn_sz,
                      enum hyperclient_returncode* status,
                      struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if (maintain_coord_connection(status) < 0)
    {
        return -1;
    }

    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dimension_names = m_config->dimension_names(si);
    assert(dimension_names.size() > 0);
    e::bitfield seen(dimension_names.size());

    // Populate the search object.
    hyperspacehashing::search s(dimension_names.size());

    // Check the equality conditions.
    for (size_t i = 0; i < eq_sz; ++i)
    {
        std::vector<hyperdex::attribute>::const_iterator dim;
        dim = dimension_names.begin();

        while (dim < dimension_names.end() && dim->name != eq[i].attr)
        {
            ++dim;
        }

        if (dim == dimension_names.begin())
        {
            *status = HYPERCLIENT_DONTUSEKEY;
            return -1 - i;
        }

        if (dim == dimension_names.end())
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - i;
        }

        uint16_t dimnum = dim - dimension_names.begin();

        if (seen.get(dimnum))
        {
            *status = HYPERCLIENT_DUPEATTR;
            return -1 - i;
        }

        if (eq[i].datatype != hd_to_hc_type(dim->type))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return -1 - i;
        }

        s.equality_set(dimnum, e::slice(eq[i].value, eq[i].value_sz));
    }

    // Check the range conditions.
    for (size_t i = 0; i < rn_sz; ++i)
    {
        std::vector<hyperdex::attribute>::const_iterator dim;
        dim = dimension_names.begin();

        while (dim < dimension_names.end() && dim->name != rn[i].attr)
        {
            ++dim;
        }

        if (dim == dimension_names.end())
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - eq_sz - i;
        }

        uint16_t dimnum = dim - dimension_names.begin();

        if (seen.get(dimnum))
        {
            *status = HYPERCLIENT_DUPEATTR;
            return -1 - eq_sz - i;
        }

        if (hd_to_hc_type(dim->type) != HYPERDATATYPE_UINT64)
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return -1 - eq_sz - i;
        }

        s.range_set(dimnum, rn[i].lower, rn[i].upper);
    }

    // Get the hosts that match our search terms.
    std::map<hyperdex::entityid, hyperdex::instance> search_entities;
    search_entities = m_config->search_entities(si, s);

    // Send a search query to each matching host.
    uint64_t searchid = m_requestid;
    ++m_requestid;

    // Pack the message to send
    std::auto_ptr<e::buffer> msg(e::buffer::create(HDRSIZE + sizeof(uint64_t) + s.packed_size()));
    bool packed = !(msg->pack_at(HDRSIZE) << searchid << s).error();
    assert(packed);
    std::tr1::shared_ptr<uint64_t> refcount(new uint64_t(0));

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator ent_inst = search_entities.begin();
            ent_inst != search_entities.end(); ++ent_inst)
    {
        e::intrusive_ptr<pending> op   = new pending_search(this, searchid, refcount, status, attrs, attrs_sz);
        e::intrusive_ptr<channel> chan = get_channel(ent_inst->second, status);

        if (!chan)
        {
            m_completed.push(completedop(op, HYPERCLIENT_CONNECTFAIL));
            continue;
        }

        op->set_id(searchid);
        op->set_nonce(chan->generate_nonce());
        op->set_channel(chan);
        op->set_entity(ent_inst->first);
        op->set_instance(ent_inst->second);
        m_requests.erase(std::make_pair(chan->sock().get(), op->nonce()));
        m_requests.insert(std::make_pair(std::make_pair(chan->sock().get(), op->nonce()), op));

        if (send(chan, op, msg.get()) < 0)
        {
            m_requests.erase(std::make_pair(chan->sock().get(), op->nonce()));
        }
    }

    return searchid;
}

int64_t
hyperclient :: loop(int timeout, hyperclient_returncode* status)
{
    while (!m_requests.empty() && m_completed.empty())
    {
        if (maintain_coord_connection(status) < 0)
        {
            return -1;
        }

        epoll_event ee;
        int polled = epoll_wait(m_epfd.get(), &ee, 1, timeout);

        if (polled < 0)
        {
            *status = HYPERCLIENT_SEEERRNO;
            return -1;
        }

        if (polled == 0)
        {
            *status = HYPERCLIENT_TIMEOUT;
            return -1;
        }

        if (ee.data.fd == m_coord->pfd().fd)
        {
            switch (m_coord->loop(1, 0))
            {
                case hyperdex::coordinatorlink::SUCCESS:
                    break;
                case hyperdex::coordinatorlink::CONNECTFAIL:
                case hyperdex::coordinatorlink::DISCONNECT:
                    *status = HYPERCLIENT_COORDFAIL;
                    return -1;
                case hyperdex::coordinatorlink::SHUTDOWN:
                case hyperdex::coordinatorlink::LOGICERROR:
                default:
                    *status = HYPERCLIENT_LOGICERROR;
                    return -1;
            }

            if (m_coord->unacknowledged())
            {
                *m_config = m_coord->config();
                requests_map_t::iterator r = m_requests.begin();

                while (r != m_requests.end())
                {
                    if (m_config->instancefor(r->second->entity()) != r->second->instance())
                    {
                        m_completed.push(completedop(r->second, HYPERCLIENT_RECONFIGURE));
                        m_requests.erase(r);
                        r = m_requests.begin();
                    }
                    else
                    {
                        ++r;
                    }
                }

                // This must only happen if we fail everything with a
                // reconfigure.
                m_coord->acknowledge();
            }

            continue;
        }

        e::intrusive_ptr<channel> chan = channel_from_fd(ee.data.fd);
        assert(chan->sock().get() == ee.data.fd);

        if ((ee.events & EPOLLHUP) || (ee.events & EPOLLERR))
        {
            killall(ee.data.fd, HYPERCLIENT_DISCONNECT);
            m_coord->fail_location(chan->location());
            continue;
        }

        if (!(ee.events & EPOLLIN))
        {
            continue;
        }

        uint32_t size;
        ssize_t ret = chan->sock().recv(&size, sizeof(size), MSG_DONTWAIT|MSG_PEEK);

        if (ret < 0 || ret == 0)
        {
            killall(ee.data.fd, HYPERCLIENT_DISCONNECT);
            m_coord->fail_location(chan->location());
            continue;
        }
        else if (ret != sizeof(size))
        {
            continue;
        }

        size = be32toh(size);
        std::auto_ptr<e::buffer> response(e::buffer::create(size));

        if (chan->sock().xrecv(response->data(), size, 0) < size)
        {
            killall(ee.data.fd, HYPERCLIENT_DISCONNECT);
            m_coord->fail_location(chan->location());
            continue;
        }

        response->resize(size);

        uint8_t type_num;
        uint64_t version;
        uint16_t fromver;
        uint16_t tover;
        hyperdex::entityid from;
        hyperdex::entityid to;
        uint64_t nonce;
        e::buffer::unpacker up = response->unpack_from(sizeof(size));
        up = up >> type_num >> version >> fromver >> tover >> from >> to >> nonce;

        if (up.error())
        {
            killall(ee.data.fd, HYPERCLIENT_SERVERERROR);
            continue;
        }

        hyperdex::network_msgtype msg_type = static_cast<hyperdex::network_msgtype>(type_num);

        if (chan->entity() == hyperdex::entityid(hyperdex::configuration::CLIENTSPACE, 0, 0, 0, 0))
        {
            chan->set_entity(to);
        }

        requests_map_t::iterator it = m_requests.find(std::make_pair(chan->sock().get(), nonce));

        if (it == m_requests.end())
        {
            killall(ee.data.fd, HYPERCLIENT_SERVERERROR);
            continue;
        }

        e::intrusive_ptr<pending> op = it->second;
        assert(op);
        assert(chan == op->chan());
        assert(nonce == op->nonce());

        if (msg_type == hyperdex::CONFIGMISMATCH)
        {
            op->set_status(HYPERCLIENT_RECONFIGURE);
            m_requests.erase(it);
            return op->id();
        }

        if (fromver == op->instance().inbound_version &&
            tover == 1 &&
            from == op->entity() &&
            to == chan->entity())
        {
            if (!op->matches_response_type(msg_type))
            {
                killall(ee.data.fd, HYPERCLIENT_SERVERERROR);
                break;
            }

            switch (op->handle_response(msg_type, response.get(), status))
            {
                case KEEP:
                    return op->id();
                case SILENTREMOVE:
                    m_requests.erase(it);
                    break;
                case REMOVE:
                    m_requests.erase(it);
                    return op->id();
                case FAIL:
                    killall(ee.data.fd, *status);
                    break;
                default:
                    abort();
            }

            // We cannot process more than one event.  The first
            // event processed will either return above, or kill a
            // number of pending operations.  By breaking here, we
            // force another iteration of the global while loop,
            // where we can return the failed operation(s).
        }
        else
        {
            killall(ee.data.fd, HYPERCLIENT_SERVERERROR);
            break;
        }
    }

    if (m_completed.empty())
    {
        *status = HYPERCLIENT_NONEPENDING;
        return -1;
    }
    else
    {
        *status = HYPERCLIENT_SUCCESS;
        int64_t ret = m_completed.front().op->id();
        m_completed.front().op->set_status(m_completed.front().why);
        m_completed.pop();
        return ret;
    }
}

int64_t
hyperclient :: add_keyop(const char* space, const char* key, size_t key_sz,
                         std::auto_ptr<e::buffer> msg, e::intrusive_ptr<pending> op)
{
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        op->set_status(HYPERCLIENT_UNKNOWNSPACE);
        return -1;
    }

    // Figure out who to talk with.
    hyperdex::entityid dst_ent;
    hyperdex::instance dst_inst;

    if (!m_config->point_leader_entity(si, e::slice(key, key_sz), &dst_ent, &dst_inst))
    {
        op->set_status(HYPERCLIENT_CONNECTFAIL);
        return -1;
    }

    hyperclient_returncode status;
    e::intrusive_ptr<channel> chan = get_channel(dst_inst, &status);

    if (!chan)
    {
        op->set_status(status);
        return -1;
    }

    op->set_nonce(chan->generate_nonce());
    op->set_channel(chan);
    op->set_entity(dst_ent);
    op->set_instance(dst_inst);
    m_requests.erase(std::make_pair(chan->sock().get(), op->nonce()));
    m_requests.insert(std::make_pair(std::make_pair(chan->sock().get(), op->nonce()), op));

    typedef size_t (requests_map_t::*functype)(const std::pair<int, uint64_t>& t);
    functype f = &requests_map_t::erase;
    e::guard remove_op = e::makeobjguard(m_requests, f, std::make_pair(chan->sock().get(), op->nonce()));
    e::guard fail_op = e::makeobjguard(*op, &pending::set_status, HYPERCLIENT_SEEERRNO);
    int64_t ret = send(chan, op, msg.get());
    assert(ret <= 0);

    // We dismiss the remove_op guard when the operation succeeds within send.
    if (ret == 0)
    {
        remove_op.dismiss();
        op->set_id(m_requestid);
        ret = op->id();
        ++m_requestid;
    }

    // We dismiss the fail_op guard no matter what because if ret < 0, then send
    // already set the status to a more-detailed reason.
    fail_op.dismiss();
    return ret;
}

int64_t
hyperclient :: pack_attrs(const char* space, e::buffer::packer* p,
                          const struct hyperclient_attribute* attrs,
                          size_t attrs_sz, hyperclient_returncode* status)
{
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<hyperdex::attribute> dimension_names = m_config->dimension_names(si);
    assert(dimension_names.size() > 0);
    e::bitfield seen(dimension_names.size());
    uint32_t sz = attrs_sz;
    *p = *p << sz;

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        std::vector<hyperdex::attribute>::const_iterator dim;
        dim = dimension_names.begin();

        while (dim < dimension_names.end() && dim->name != attrs[i].attr)
        {
            ++dim;
        }

        if (dim == dimension_names.begin())
        {
            *status = HYPERCLIENT_DONTUSEKEY;
            return -1 - i;
        }

        if (dim == dimension_names.end())
        {
            *status = HYPERCLIENT_UNKNOWNATTR;
            return -1 - i;
        }

        uint16_t dimnum = dim - dimension_names.begin();

        if (seen.get(dimnum))
        {
            *status = HYPERCLIENT_DUPEATTR;
            return -1 - i;
        }

        if (attrs[i].datatype != hd_to_hc_type(dim->type))
        {
            *status = HYPERCLIENT_WRONGTYPE;
            return -1 - i;
        }

        *p = *p << dimnum << e::slice(attrs[i].value, attrs[i].value_sz);
        seen.set(dimnum);
    }

    assert(!(*p).error());
    return 0;
}

size_t
hyperclient :: pack_attrs_sz(const struct hyperclient_attribute* attrs,
                             size_t attrs_sz)
{
    size_t sz = sizeof(uint32_t) + attrs_sz * (sizeof(uint16_t) + sizeof(uint32_t));

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        sz += attrs[i].value_sz;
    }

    return sz;
}

int64_t
hyperclient :: send(e::intrusive_ptr<channel> chan,
                    e::intrusive_ptr<pending> op,
                    e::buffer* payload)
{
    const uint8_t type = static_cast<uint8_t>(op->request_type());
    const uint64_t version = m_config->version();
    const uint16_t fromver = 1;
    const uint16_t tover = op->instance().inbound_version;
    const hyperdex::entityid& from(chan->entity());
    const hyperdex::entityid& to(op->entity());
    const uint64_t nonce = op->nonce();
    const uint32_t size = payload->size();
    e::buffer::packer pa = payload->pack();
    pa = pa << size << type << version << fromver << tover << from << to << nonce;
    assert(!pa.error());

    if (chan->sock().xsend(payload->data(), payload->size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(payload->size()))
    {
        killall(chan->sock().get(), HYPERCLIENT_DISCONNECT);
        m_coord->fail_location(chan->location());
        return -1;
    }

    return 0;
}

int64_t
hyperclient :: maintain_coord_connection(hyperclient_returncode* status)
{
    switch (m_coord->connect())
    {
        case hyperdex::coordinatorlink::SUCCESS:
            break;
        case hyperdex::coordinatorlink::CONNECTFAIL:
            *status = HYPERCLIENT_COORDFAIL;
            return -1;
        case hyperdex::coordinatorlink::DISCONNECT:
        case hyperdex::coordinatorlink::SHUTDOWN:
        case hyperdex::coordinatorlink::LOGICERROR:
        default:
            *status = HYPERCLIENT_LOGICERROR;
            return -1;
    }

    epoll_event ee;
    ee.events = EPOLLIN;
    ee.data.fd = m_coord->pfd().fd;

    if (epoll_ctl(m_epfd.get(), EPOLL_CTL_ADD, m_coord->pfd().fd, &ee) < 0 && errno != EEXIST)
    {
        *status = HYPERCLIENT_COORDFAIL;
        return -1;
    }

    do
    {
        switch (m_coord->loop(1, m_have_seen_config ? 0 : -1))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
            case hyperdex::coordinatorlink::DISCONNECT:
                *status = HYPERCLIENT_COORDFAIL;
                return -1;
            case hyperdex::coordinatorlink::SHUTDOWN:
            case hyperdex::coordinatorlink::LOGICERROR:
            default:
                *status = HYPERCLIENT_LOGICERROR;
                return -1;
        }

        if (m_coord->unacknowledged())
        {
            *m_config = m_coord->config();
            m_coord->acknowledge();
            m_have_seen_config = true;
        }
    }
    while (!m_have_seen_config);

    return 0;
}

void
hyperclient :: killall(int fd, hyperclient_returncode status)
{
    assert(0 <= fd && static_cast<unsigned>(fd) < m_fds.size());
    e::intrusive_ptr<channel> chan = m_fds[fd];
    m_fds[fd] = NULL;

    if (!chan)
    {
        return;
    }

    requests_map_t::iterator r = m_requests.begin();

    while (r != m_requests.end())
    {
        if (r->second->chan() == chan)
        {
            m_completed.push(completedop(r->second, status));
            m_requests.erase(r);
            r = m_requests.begin();
        }
        else
        {
            ++r;
        }
    }

    for (instances_map_t::iterator i = m_instances.begin();
            i != m_instances.end(); ++i)
    {
        if (i->second == chan)
        {
            // There is only one instance for this channel.  Erase it and break.
            m_instances.erase(i);
            break;
        }
    }

    try
    {
        epoll_event ee;
        epoll_ctl(m_epfd.get(), EPOLL_CTL_DEL, chan->sock().get(), &ee);
        chan->sock().shutdown(SHUT_RDWR);
        chan->sock().close();
    }
    catch (po6::error& e)
    {
        // silence errors
    }
}

e::intrusive_ptr<hyperclient::channel>
hyperclient :: get_channel(hyperdex::instance inst,
                           hyperclient_returncode* status)
{
    std::map<hyperdex::instance, e::intrusive_ptr<channel> >::iterator i;
    i = m_instances.find(inst);

    if (i != m_instances.end())
    {
        return i->second;
    }

    po6::net::location loc(inst.address, inst.inbound_port);

    try
    {
        e::intrusive_ptr<channel> chan = new channel(loc);
        epoll_event ee;
        ee.events = EPOLLIN;
        ee.data.fd = chan->sock().get();

        if (epoll_ctl(m_epfd.get(), EPOLL_CTL_ADD, chan->sock().get(), &ee) < 0)
        {
            *status = HYPERCLIENT_CONNECTFAIL;
            m_coord->warn_location(chan->location());
            return NULL;
        }

        m_instances[inst] = chan;
        m_fds[chan->sock().get()] = chan;
        return chan;
    }
    catch (po6::error& e)
    {
        *status = HYPERCLIENT_CONNECTFAIL;
        m_coord->warn_location(loc);
        errno = e;
        return NULL;
    }
}

e::intrusive_ptr<hyperclient::channel>
hyperclient :: channel_from_fd(int fd)
{
    assert(0 <= fd && static_cast<unsigned>(fd) < m_fds.size());
    return m_fds[fd];
}

#define str(x) #x
#define xstr(x) str(x)
#define stringify(x) case (x): lhs << xstr(x); break

std::ostream&
operator << (std::ostream& lhs, hyperclient_returncode rhs)
{
    switch (rhs)
    {
        stringify(HYPERCLIENT_SUCCESS);
        stringify(HYPERCLIENT_NOTFOUND);
        stringify(HYPERCLIENT_SEARCHDONE);
        stringify(HYPERCLIENT_CMPFAIL);
        stringify(HYPERCLIENT_UNKNOWNSPACE);
        stringify(HYPERCLIENT_COORDFAIL);
        stringify(HYPERCLIENT_SERVERERROR);
        stringify(HYPERCLIENT_CONNECTFAIL);
        stringify(HYPERCLIENT_DISCONNECT);
        stringify(HYPERCLIENT_RECONFIGURE);
        stringify(HYPERCLIENT_LOGICERROR);
        stringify(HYPERCLIENT_TIMEOUT);
        stringify(HYPERCLIENT_UNKNOWNATTR);
        stringify(HYPERCLIENT_DUPEATTR);
        stringify(HYPERCLIENT_SEEERRNO);
        stringify(HYPERCLIENT_NONEPENDING);
        stringify(HYPERCLIENT_DONTUSEKEY);
        stringify(HYPERCLIENT_WRONGTYPE);
        stringify(HYPERCLIENT_EXCEPTION);
        stringify(HYPERCLIENT_ZERO);
        stringify(HYPERCLIENT_A);
        stringify(HYPERCLIENT_B);
        default:
            lhs << "unknown returncode";
            break;
    }

    return lhs;
}

#undef stringify
#undef xstr
#undef str
