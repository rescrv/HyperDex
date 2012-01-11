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

// e
#include <e/guard.h>

// HyperDex
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/instance.h"
#include "hyperdex/hyperdex/network_constants.h"

// HyperClient
#include "hyperclient/hyperclient.h"

#define HDRSIZE (sizeof(uint32_t) + sizeof(uint8_t) + 2 * sizeof(uint16_t) + 2 * hyperdex::entityid::SERIALIZEDSIZE + sizeof(uint64_t))

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
                   struct hyperclient_search_result** results)
{
    return -1;
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

} // extern "C"

///////////////////////////////// Channel Class ////////////////////////////////

class hyperclient::channel
{
    public:
        channel(const hyperdex::instance& inst);

    public:
        // The entity by which the remote host knows us
        const hyperdex::entityid& entity() const { return m_ent; }

    public:
        uint64_t generate_nonce() { return m_nonce++; }
        void set_nonce(uint64_t nonce) { m_nonce = nonce; }
        void set_entity(hyperdex::entityid ent) { m_ent = ent; }
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
        po6::net::socket m_sock;
};

hyperclient :: channel :: channel(const hyperdex::instance& inst)
    : m_ref(0)
    , m_nonce(1)
    , m_ent(hyperdex::configuration::CLIENTSPACE)
    , m_sock(inst.inbound.address.family(), SOCK_STREAM, IPPROTO_TCP)
{
    m_sock.connect(inst.inbound);
    m_sock.set_tcp_nodelay();
}

hyperclient :: channel :: ~channel() throw ()
{
}

//////////////////////////////// Failedop Class ////////////////////////////////

class hyperclient::failedop
{
    public:
        failedop(e::intrusive_ptr<pending> _op, hyperclient_returncode _why)
            : op(_op), why(_why) {}
        failedop(const failedop& other)
            : op(other.op), why(other.why) {}
        ~failedop() throw () {}

    public:
        failedop& operator = (const failedop& rhs)
        { op = rhs.op; why = rhs.why; return *this; }

    public:
        e::intrusive_ptr<pending> op;
        hyperclient_returncode why;
};

///////////////////////////////// Pending Class ////////////////////////////////

enum handled_how
{
    KEEP,
    REMOVE,
    FAIL
};

class hyperclient::pending
{
    public:
        pending(hyperdex::network_msgtype reqtype,
                hyperdex::network_msgtype resptype,
                hyperclient_returncode* status);
        virtual ~pending() throw ();

    public:
        hyperdex::network_msgtype request_type() const { return m_reqtype; }
        hyperdex::network_msgtype response_type() const { return m_resptype; }
        int64_t id() const { return m_id; }
        uint64_t nonce() const { return m_nonce; }
        e::intrusive_ptr<channel> chan() const { return m_chan; }
        const hyperdex::entityid& entity() const { return m_ent; }
        const hyperdex::instance& instance() const { return m_inst; }

    public:
        virtual handled_how handle_response(std::auto_ptr<e::buffer> msg,
                                            hyperclient_returncode* status) = 0;
        void set_status(hyperclient_returncode status) { *m_status = status; }
        void set_id(int64_t _id) { m_id = _id; }
        void set_nonce(uint64_t _nonce) { m_nonce = _nonce; }
        void set_channel(e::intrusive_ptr<channel> _chan) { m_chan = _chan; }
        void set_entity(hyperdex::entityid ent) { m_ent = ent; }
        void set_instance(hyperdex::instance inst) { m_inst = inst; }

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
        hyperclient_returncode* m_status;
        hyperdex::network_msgtype m_reqtype;
        hyperdex::network_msgtype m_resptype;
        e::intrusive_ptr<channel> m_chan;
        int64_t m_id;
        uint64_t m_nonce;
        hyperdex::entityid m_ent;
        hyperdex::instance m_inst;
        bool m_finished;
};

hyperclient :: pending :: pending(hyperdex::network_msgtype reqtype,
                                  hyperdex::network_msgtype resptype,
                                  hyperclient_returncode* status)
    : m_ref(0)
    , m_status(status)
    , m_reqtype(reqtype)
    , m_resptype(resptype)
    , m_chan()
    , m_id(-1)
    , m_nonce(0)
    , m_ent()
    , m_inst()
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
        handled_how handle_response(std::auto_ptr<e::buffer> msg,
                                    hyperclient_returncode* status);

    private:
        pending_get(const pending_get& other);

    private:
        pending_get& operator = (const pending_get& rhs);

    private:
        hyperclient* m_cl;
        struct hyperclient_attribute** m_attrs;
        size_t* m_attrs_sz;
};

hyperclient :: pending_get :: pending_get(hyperclient* cl,
                                          hyperclient_returncode* status,
                                          struct hyperclient_attribute** attrs,
                                          size_t* attrs_sz)
    : pending(hyperdex::REQ_GET, hyperdex::RESP_GET, status)
    , m_cl(cl)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
}

hyperclient :: pending_get :: ~pending_get() throw ()
{
}

handled_how
hyperclient :: pending_get :: handle_response(std::auto_ptr<e::buffer> msg,
                                              hyperclient_returncode*)
{
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
        case hyperdex::NET_WRONGARITY:
            set_status(HYPERCLIENT_LOGICERROR);
            return REMOVE;
        case hyperdex::NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            return REMOVE;
        case hyperdex::NET_SERVERERROR:
        default:
            set_status(HYPERCLIENT_SERVERERROR);
            return REMOVE;
    }

    std::vector<std::string> dimension_names = m_cl->m_config->dimension_names(entity().get_space());
    std::vector<e::slice> value;
    up = up >> value;

    if (up.error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        return REMOVE;
    }

    if (value.size() + 1 != dimension_names.size())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        return REMOVE;
    }

    std::vector<hyperclient_attribute> ha(value.size());
    size_t sz = sizeof(hyperclient_attribute) * value.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        sz += dimension_names[i + 1].size() + 1 + value[i].size();
    }

    char* ret = static_cast<char*>(malloc(sz));

    if (!ret)
    {
        set_status(HYPERCLIENT_SEEERRNO);
        return REMOVE;
    }

    e::guard g = e::makeguard(free, ret);
    char* data = ret + sizeof(hyperclient_attribute) * value.size();

    for (size_t i = 0; i < value.size(); ++i)
    {
        size_t attr_sz = dimension_names[i + 1].size() + 1;
        ha[i].attr = data;
        memmove(data, dimension_names[i + 1].c_str(), attr_sz);
        data += attr_sz;
        ha[i].value = data;
        memmove(data, value[i].data(), value[i].size());
        data += value[i].size();
        ha[i].value_sz = value[i].size();
    }

    memmove(ret, &ha.front(), sizeof(hyperclient_attribute) * value.size());
    *m_attrs = reinterpret_cast<hyperclient_attribute*>(ret);
    *m_attrs_sz = value.size();
    set_status(HYPERCLIENT_SUCCESS);
    g.dismiss();
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
        handled_how handle_response(std::auto_ptr<e::buffer> msg,
                                    hyperclient_returncode* status);
};

hyperclient :: pending_statusonly :: pending_statusonly(
                            hyperdex::network_msgtype reqtype,
                            hyperdex::network_msgtype resptype,
                            hyperclient_returncode* status)
    : pending(reqtype, resptype, status)
{
}

hyperclient :: pending_statusonly :: ~pending_statusonly() throw ()
{
}

handled_how
hyperclient :: pending_statusonly :: handle_response(std::auto_ptr<e::buffer> msg,
                                                     hyperclient_returncode*)
{
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
        case hyperdex::NET_WRONGARITY:
            set_status(HYPERCLIENT_LOGICERROR);
            break;
        case hyperdex::NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            break;
        case hyperdex::NET_SERVERERROR:
        default:
            set_status(HYPERCLIENT_SERVERERROR);
            break;
    }

    return REMOVE;
}

///////////////////////////////// Public Class /////////////////////////////////

hyperclient :: hyperclient(const char* coordinator, in_port_t port)
    : m_coord(new hyperdex::coordinatorlink(po6::net::location(coordinator, port)))
    , m_config(new hyperdex::configuration())
    , m_fds(sysconf(_SC_OPEN_MAX), e::intrusive_ptr<channel>())
    , m_instances()
    , m_requests()
    , m_requestid(1)
    , m_failed()
    , m_configured(false)
{
    m_coord->set_announce("client");
}

hyperclient :: ~hyperclient() throw ()
{
}

int64_t
hyperclient :: get(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status,
                   struct hyperclient_attribute** attrs, size_t* attrs_sz)
{
    if ((!m_configured || !m_coord->connected()) && try_coord_connect(status) < 0)
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
    if ((!m_configured || !m_coord->connected()) && try_coord_connect(status) < 0)
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

    int64_t ret = pack_attrs(space, p, attrs, attrs_sz, status);

    if (ret < 0)
    {
        return ret;
    }

    return add_keyop(space, key, key_sz, msg, op);
}

int64_t
hyperclient :: del(const char* space, const char* key, size_t key_sz,
                   hyperclient_returncode* status)
{
    if ((!m_configured || !m_coord->connected()) && try_coord_connect(status) < 0)
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
hyperclient :: loop(int timeout, hyperclient_returncode* status)
{
    while (!m_requests.empty() && m_failed.empty())
    {
        if (!m_requests.front())
        {
            m_requests.pop_front();
            continue;
        }

        if ((!m_configured || !m_coord->connected()) && try_coord_connect(status) < 0)
        {
            return -1;
        }

        std::vector<pollfd> pfds;
        std::set<int> seen;
        pfds.reserve(m_requests.size());

        for (requests_list_t::iterator r = m_requests.begin();
                r != m_requests.end(); ++r)
        {
            if (*r && seen.find((*r)->chan()->sock().get()) == seen.end())
            {
                pfds.push_back(pollfd());
                pfds.back().fd = (*r)->chan()->sock().get();
                pfds.back().events = POLLIN;
                pfds.back().revents = 0;
                seen.insert((*r)->chan()->sock().get());
            }
        }

        pfds.push_back(m_coord->pfd());
        pfds.back().revents = 0;

        int polled = poll(&pfds.front(), pfds.size(), timeout);

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

        if (pfds.back().revents)
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

                for (requests_list_t::iterator r = m_requests.begin();
                        r != m_requests.end(); ++r)
                {
                    if (*r && m_config->instancefor((*r)->entity()) != (*r)->instance())
                    {
                        m_failed.push(failedop(*r, HYPERCLIENT_RECONFIGURE));
                        *r = NULL;
                    }
                }

                // This must only happen if we fail everything with a
                // reconfigure.
                m_coord->acknowledge();
                continue;
            }
        }

        for (size_t i = 0; i < pfds.size(); ++i)
        {
            if ((pfds[i].revents & POLLHUP) || (pfds[i].revents & POLLERR))
            {
                killall(pfds[i].fd, HYPERCLIENT_DISCONNECT);
                continue;
            }

            if (!(pfds[i].revents & POLLIN))
            {
                continue;
            }

            e::intrusive_ptr<channel> chan = channel_from_fd(pfds[i].fd);
            assert(chan->sock().get() >= 0);

            uint32_t size;

            if (chan->sock().recv(&size, sizeof(size), MSG_DONTWAIT|MSG_PEEK) !=
                    sizeof(size))
            {
                continue;
            }

            size = be32toh(size);
            std::auto_ptr<e::buffer> response(e::buffer::create(size));

            if (chan->sock().xrecv(response->data(), size, 0) < size)
            {
                killall(pfds[i].fd, HYPERCLIENT_DISCONNECT);
                continue;
            }

            response->resize(size);

            uint8_t type_num;
            uint16_t fromver;
            uint16_t tover;
            hyperdex::entityid from;
            hyperdex::entityid to;
            uint64_t nonce;
            e::buffer::unpacker up = response->unpack_from(sizeof(size));
            up = up >> type_num >> fromver >> tover >> from >> to >> nonce;

            if (up.error())
            {
                killall(pfds[i].fd, HYPERCLIENT_SERVERERROR);
                continue;;
            }

            hyperdex::network_msgtype msg_type = static_cast<hyperdex::network_msgtype>(type_num);

            if (chan->entity() == hyperdex::entityid(hyperdex::configuration::CLIENTSPACE))
            {
                chan->set_entity(to);
            }

            for (requests_list_t::iterator r = m_requests.begin();
                    r != m_requests.end(); ++r)
            {
                e::intrusive_ptr<pending> op = *r;

                if (op &&
                    chan == op->chan() &&
                    fromver == op->instance().inbound_version &&
                    tover == 1 &&
                    from == op->entity() &&
                    to == chan->entity() &&
                    nonce == op->nonce())
                {
                    if (msg_type != op->response_type())
                    {
                        killall(pfds[i].fd, HYPERCLIENT_SERVERERROR);
                        break;
                    }

                    // XXX How will this deal with searches?
                    switch (op->handle_response(response, status))
                    {
                        case KEEP:
                            return op->id();
                        case REMOVE:
                            *r = NULL;
                            return op->id();
                        case FAIL:
                            killall(pfds[i].fd, *status);
                            break;
                        default:
                            abort();
                    }

                    // We cannot process more than one event.  The first
                    // event processed will either return above, or kill a
                    // number of pending operations.  By breaking here, we
                    // force another iteration of the global while loop,
                    // where we can return the failed operation(s).
                    break;
                }
            }
        }
    }

    if (m_failed.empty())
    {
        *status = HYPERCLIENT_NONEPENDING;
        return -1;
    }
    else
    {
        *status = HYPERCLIENT_SUCCESS;
        int64_t ret = m_failed.front().op->id();
        m_failed.front().op->set_status(m_failed.front().why);
        m_failed.pop();
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
    m_requests.push_back(op);
    e::guard remove_op = e::makeobjguard(m_requests, &requests_list_t::pop_back);
    e::guard fail_op = e::makeobjguard(*op, &pending::set_status, HYPERCLIENT_SEEERRNO);
    int64_t ret = send(chan, op, msg);
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
hyperclient :: pack_attrs(const char* space, e::buffer::packer p,
                          const struct hyperclient_attribute* attrs,
                          size_t attrs_sz, hyperclient_returncode* status)
{
    hyperdex::spaceid si = m_config->space(space);

    if (si == hyperdex::spaceid())
    {
        *status = HYPERCLIENT_UNKNOWNSPACE;
        return -1;
    }

    std::vector<std::string> dimension_names = m_config->dimension_names(si);
    assert(dimension_names.size() > 0);
    e::bitfield seen(dimension_names.size());
    uint32_t sz = attrs_sz;
    p = p << sz;

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        std::vector<std::string>::const_iterator dim;
        dim = std::find(dimension_names.begin(), dimension_names.end(), attrs[i].attr);

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

        p = p << dimnum << e::slice(attrs[i].value, attrs[i].value_sz);
        seen.set(dimnum);
    }

    assert(!p.error());
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
                    std::auto_ptr<e::buffer> payload)
{
    const uint8_t type = static_cast<uint8_t>(op->request_type());
    const uint16_t fromver = 1;
    const uint16_t tover = op->instance().inbound_version;
    const hyperdex::entityid& from(chan->entity());
    const hyperdex::entityid& to(op->entity());
    const uint64_t nonce = op->nonce();
    const uint32_t size = payload->size();
    e::buffer::packer pa = payload->pack();
    pa = pa << size << type << fromver << tover << from << to << nonce;
    assert(!pa.error());

    if (chan->sock().xsend(payload->data(), payload->size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(payload->size()))
    {
        killall(chan->sock().get(), HYPERCLIENT_DISCONNECT);
        return -1;
    }

    return 0;
}

int64_t
hyperclient :: try_coord_connect(hyperclient_returncode* status)
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

    while (!m_configured)
    {
        switch (m_coord->loop(1, -1))
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
            m_configured = true;
        }
    }

    return 0;
}

void
hyperclient :: killall(int fd, hyperclient_returncode status)
{
    assert(0 <= fd && static_cast<unsigned>(fd) < m_fds.size());
    e::intrusive_ptr<channel> chan = m_fds[fd];

    for (requests_list_t::iterator r = m_requests.begin();
            r != m_requests.end(); ++r)
    {
        if ((*r)->chan() == chan)
        {
            m_failed.push(failedop(*r, status));
            *r = NULL;
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

    m_fds[fd] = NULL;
    chan->sock().shutdown(SHUT_RDWR);
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

    try
    {
        e::intrusive_ptr<channel> chan = new channel(inst);
        m_instances[inst] = chan;
        m_fds[chan->sock().get()] = chan;
        return chan;
    }
    catch (po6::error& e)
    {
        *status = HYPERCLIENT_CONNECTFAIL;
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
