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

// STL
#include <map>

// Google CityHash
#include "../../cityhash/include/city.h"

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/coordinatorlink.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/ids.h>
#include <hyperdex/network_constants.h>
#include <hyperdex/search.h>

// HyperClient
#include "../include/hyperclient/client.h"

class hyperclient :: client :: priv
{
    public:
        class channel;

    public:
        priv(const po6::net::location& coordinator);
        ~priv() throw ();

    public:
        bool find_entity(const hyperdex::regionid& r, hyperdex::entityid* ent, hyperdex::instance* inst);
        hyperclient::status send(e::intrusive_ptr<channel> chan,
                                 const hyperdex::instance& inst,
                                 const hyperdex::entityid& ent,
                                 hyperdex::network_msgtype type,
                                 uint32_t nonce,
                                 const e::buffer& msg);
        hyperclient::status recv(e::intrusive_ptr<channel> chan,
                                 const hyperdex::instance& inst,
                                 const hyperdex::entityid& ent,
                                 hyperdex::network_msgtype resp_type,
                                 uint32_t expected_nonce,
                                 hyperdex::network_returncode* resp_stat,
                                 e::buffer* resp_msg);
        hyperclient::status reqrep(const std::string& space,
                                   const e::buffer& key,
                                   hyperdex::network_msgtype req_type,
                                   hyperdex::network_msgtype resp_type,
                                   const e::buffer& msg,
                                   hyperdex::network_returncode* resp_status,
                                   e::buffer* resp_msg);

    public:
        hyperdex::coordinatorlink cl;
        hyperdex::configuration config;
        std::map<hyperdex::instance, e::intrusive_ptr<channel> > channels;
};

class hyperclient :: client :: priv :: channel
{
    public:
        channel(const hyperdex::instance& inst);

    public:
        po6::net::socket soc;
        uint64_t nonce;
        hyperdex::entityid id;

    private:
        friend class e::intrusive_ptr<channel>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
};

class hyperclient::client::search_results::priv
{
    public:
        typedef e::intrusive_ptr<client::priv::channel> channel_ptr;

    public:
        priv();
        priv(client::priv* c,
             std::map<hyperdex::entityid, hyperdex::instance>* hosts,
             std::map<hyperdex::instance, channel_ptr>* chansubset,
             const hyperdex::search& s);
        ~priv() throw ();

    public:
        status next();

    public:
        client::priv* c;
        std::map<hyperdex::entityid, hyperdex::instance> hosts;
        std::map<hyperdex::instance, channel_ptr> chansubset;
        std::map<hyperdex::entityid, uint32_t> nonces;
        std::vector<pollfd> pfds;
        std::vector<hyperdex::instance> pfds_idx;
        bool valid;
        e::buffer key;
        std::vector<e::buffer> value;

    private:
        priv(const priv&);

    private:
        void complete(const hyperdex::entityid& ent);
        void disconnect(const hyperdex::instance& inst);
        void disconnect(const hyperdex::instance& inst, channel_ptr chan);

    private:
        priv& operator = (const priv&);
};

hyperclient :: client :: client(po6::net::location coordinator)
    : p(new priv(coordinator))
{
}

hyperclient::status
hyperclient :: client :: connect()
{
    switch (p->cl.connect())
    {
        case hyperdex::coordinatorlink::SUCCESS:
            break;
        case hyperdex::coordinatorlink::CONNECTFAIL:
            return COORDFAIL;
        case hyperdex::coordinatorlink::DISCONNECT:
        case hyperdex::coordinatorlink::SHUTDOWN:
        case hyperdex::coordinatorlink::LOGICERROR:
        default:
            return LOGICERROR;
    }

    while (true)
    {
        switch (p->cl.loop(1, -1))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                return COORDFAIL;
            case hyperdex::coordinatorlink::DISCONNECT:
                return COORDFAIL;
            case hyperdex::coordinatorlink::SHUTDOWN:
            case hyperdex::coordinatorlink::LOGICERROR:
            default:
                return LOGICERROR;
        }

        if (p->cl.unacknowledged())
        {
            p->config = p->cl.config();
            p->cl.acknowledge();
            break;
        }
    }

    return SUCCESS;
}

hyperclient::status
hyperclient :: client :: get(const std::string& space,
                             const e::buffer& key,
                             std::vector<e::buffer>* value)
{
    hyperdex::network_returncode resp_status;
    e::buffer resp_msg;
    status stat = p->reqrep(space, key, hyperdex::REQ_GET, hyperdex::RESP_GET, key, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case hyperdex::NET_SUCCESS:
                resp_msg.unpack() >> *value;
                return SUCCESS;
            case hyperdex::NET_NOTFOUND:
                return NOTFOUND;
            case hyperdex::NET_WRONGARITY:
                return WRONGARITY;
            case hyperdex::NET_NOTUS:
                return LOGICERROR;
            case hyperdex::NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperclient::status
hyperclient :: client :: put(const std::string& space,
                             const e::buffer& key,
                             const std::vector<e::buffer>& value)
{
    e::buffer msg;
    msg.pack() << key << value;
    hyperdex::network_returncode resp_status;
    e::buffer resp_msg;
    status stat = p->reqrep(space, key, hyperdex::REQ_PUT, hyperdex::RESP_PUT, msg, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case hyperdex::NET_SUCCESS:
                return SUCCESS;
            case hyperdex::NET_NOTFOUND:
                return NOTFOUND;
            case hyperdex::NET_WRONGARITY:
                return WRONGARITY;
            case hyperdex::NET_NOTUS:
                return LOGICERROR;
            case hyperdex::NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperclient::status
hyperclient :: client :: del(const std::string& space,
                             const e::buffer& key)
{
    hyperdex::network_returncode resp_status;
    e::buffer resp_msg;
    status stat = p->reqrep(space, key, hyperdex::REQ_DEL, hyperdex::RESP_DEL, key, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case hyperdex::NET_SUCCESS:
                return SUCCESS;
            case hyperdex::NET_NOTFOUND:
                return NOTFOUND;
            case hyperdex::NET_WRONGARITY:
                return WRONGARITY;
            case hyperdex::NET_NOTUS:
                return LOGICERROR;
            case hyperdex::NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperclient::status
hyperclient :: client :: search(const std::string& space,
                                const std::map<std::string, e::buffer>& params,
                                search_results* sr)
{
    std::map<std::string, hyperdex::spaceid>::const_iterator sai;

    // Map the string description to a number.
    if ((sai = p->config.space_assignment().find(space)) == p->config.space_assignment().end())
    {
        return NOTASPACE;
    }

    uint32_t spacenum = sai->second.space;
    std::map<hyperdex::spaceid, std::vector<std::string> >::const_iterator si;

    if ((si = p->config.spaces().find(hyperdex::spaceid(spacenum))) == p->config.spaces().end())
    {
        return LOGICERROR;
    }

    const std::vector<std::string>& dims(si->second);
    hyperdex::search terms(dims.size() - 1);

    for (std::map<std::string, e::buffer>::const_iterator par = params.begin();
            par != params.end(); ++par)
    {
        std::vector<std::string>::const_iterator dim;
        dim = std::find(dims.begin(), dims.end(), par->first);

        if (dim == dims.begin() || dim == dims.end())
        {
            return BADSEARCH;
        }

        terms.set((dim - dims.begin()) - 1, par->second);
    }

    typedef std::map<uint16_t, std::map<hyperdex::entityid, hyperdex::instance> > candidates_map;
    std::map<hyperdex::regionid, size_t> regions = p->config.regions();
    std::map<hyperdex::regionid, size_t>::iterator reg_current;
    std::map<hyperdex::regionid, size_t>::iterator reg_end;
    candidates_map candidates;
    reg_current = regions.lower_bound(hyperdex::regionid(spacenum, 1, 0, 0));
    reg_end = regions.upper_bound(hyperdex::regionid(spacenum, UINT16_MAX, UINT8_MAX, UINT64_MAX));
    uint16_t subspacenum = 0;
    uint64_t point;
    uint64_t mask;

    for (; reg_current != reg_end; ++reg_current)
    {
        if (reg_current->first.subspace > subspacenum)
        {
            std::vector<bool> whichdims;

            if (!p->config.dimensions(reg_current->first.get_subspace(), &whichdims))
            {
                return LOGICERROR;
            }

            point = terms.replication_point(whichdims);
            mask  = terms.replication_mask(whichdims);
        }

        uint64_t pmask = hyperdex::prefixmask(reg_current->first.prefix);

        if ((reg_current->first.mask & mask) == (point & pmask))
        {
            hyperdex::entityid ent = p->config.headof(reg_current->first);
            hyperdex::instance inst = p->config.lookup(ent);
            candidates[reg_current->first.subspace][ent] = inst;
        }
    }

    bool set = false;
    std::map<hyperdex::entityid, hyperdex::instance> hosts;

    for (candidates_map::iterator c = candidates.begin(); c != candidates.end(); ++c)
    {
        if (c->second.size() < hosts.size() || !set)
        {
            hosts.swap(c->second);
            set = true;
        }
    }

    status ret = SUCCESS;
    std::map<hyperdex::instance, e::intrusive_ptr<priv::channel> > chansubset;

    for (std::map<hyperdex::entityid, hyperdex::instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        e::intrusive_ptr<priv::channel> chan = p->channels[h->second];

        if (!chan)
        {
            try
            {
                p->channels[h->second] = chan = new priv::channel(h->second);
            }
            catch (po6::error& e)
            {
                ret = CONNECTFAIL;
                continue;
            }
        }

        if (chan)
        {
            chansubset[h->second] = chan;
        }
        else
        {
            ret = CONNECTFAIL;
        }
    }

    sr->p.reset(new search_results::priv(p.get(), &hosts, &chansubset, terms));
    return sr->next();
}

hyperclient :: client :: priv :: priv(const po6::net::location& coordinator)
    : cl(coordinator)
    , config()
    , channels()
{
    cl.set_announce("client");
}

hyperclient :: client :: priv :: ~priv() throw ()
{
}

bool
hyperclient :: client :: priv :: find_entity(const hyperdex::regionid& r,
                                             hyperdex::entityid* ent,
                                             hyperdex::instance* inst)
{
    bool found = false;

    for (std::map<hyperdex::entityid, hyperdex::instance>::const_iterator e = config.entity_mapping().begin();
            e != config.entity_mapping().end(); ++e)
    {
        if (overlap(e->first, r))
        {
            found = true;
            *ent = e->first;
            *inst = e->second;
            break;
        }
    }

    return found;
}

hyperclient::status
hyperclient :: client :: priv ::  send(e::intrusive_ptr<channel> chan,
                                       const hyperdex::instance& inst,
                                       const hyperdex::entityid& ent,
                                       hyperdex::network_msgtype type,
                                       uint32_t nonce,
                                       const e::buffer& msg)
{
    const uint8_t msg_type = static_cast<uint8_t>(type);
    const uint16_t fromver = 0;
    const uint16_t tover = inst.inbound_version;
    const hyperdex::entityid& from(chan->id);
    const hyperdex::entityid& to(ent);
    const uint32_t size = sizeof(uint32_t) + sizeof(msg_type) + sizeof(fromver)
                        + sizeof(tover) + hyperdex::entityid::SERIALIZEDSIZE * 2
                        + msg.size();
    e::buffer packed(size);
    packed.pack() << size << msg_type << fromver << tover << from << to
                  << nonce;
    packed += msg;

    try
    {
        chan->soc.xsend(packed.get(), packed.size(), MSG_NOSIGNAL);
    }
    catch (po6::error& e)
    {
        channels.erase(inst);
        return DISCONNECT;
    }

    return SUCCESS;
}

hyperclient::status
hyperclient :: client :: priv :: recv(e::intrusive_ptr<channel> chan,
                                      const hyperdex::instance& inst,
                                      const hyperdex::entityid& ent,
                                      hyperdex::network_msgtype resp_type,
                                      uint32_t expected_nonce,
                                      hyperdex::network_returncode* resp_stat,
                                      e::buffer* resp_msg)
{
    while (true)
    {
        if (!cl.connected())
        {
            switch (cl.connect())
            {
                case hyperdex::coordinatorlink::SUCCESS:
                    break;
                case hyperdex::coordinatorlink::CONNECTFAIL:
                case hyperdex::coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case hyperdex::coordinatorlink::SHUTDOWN:
                case hyperdex::coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }
        }

        pollfd pfds[2];
        pfds[0] = cl.pfd();
        pfds[0].revents = 0;
        pfds[1].fd = chan->soc.get();
        pfds[1].events = POLLIN;
        pfds[1].revents = 0;

        if (poll(pfds + 0, 2, -1) < 0)
        {
            return LOGICERROR;
        }

        if (pfds[0].revents != 0)
        {
            switch (cl.loop(1, 0))
            {
                case hyperdex::coordinatorlink::SUCCESS:
                    break;
                case hyperdex::coordinatorlink::CONNECTFAIL:
                case hyperdex::coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case hyperdex::coordinatorlink::SHUTDOWN:
                case hyperdex::coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }

            if (cl.unacknowledged())
            {
                config = cl.config();
                cl.acknowledge();

                if (cl.config().lookup(ent) != inst)
                {
                    return RECONFIGURE;
                }

                continue;
            }
        }

        if ((pfds[1].revents & POLLIN))
        {
            try
            {
                uint32_t size;

                if (chan->soc.recv(&size, 4, MSG_DONTWAIT|MSG_PEEK) != 4)
                {
                    continue;
                }

                size = be32toh(size);
                size += sizeof(uint32_t);
                e::buffer msg(size + sizeof(uint32_t));

                if (xread(&chan->soc, &msg, size) < size)
                {
                    channels.erase(inst);
                    return DISCONNECT;
                }

                uint32_t nop;
                uint8_t msg_type;
                uint16_t fromver;
                uint16_t tover;
                hyperdex::entityid from;
                hyperdex::entityid to;
                uint32_t nonce;
                uint16_t response;
                e::unpacker up(msg.unpack());
                up >> nop >> msg_type >> fromver >> tover >> from >> to
                   >> nonce;
                hyperdex::network_msgtype type = static_cast<hyperdex::network_msgtype>(msg_type);

                if (chan->id == hyperdex::entityid(UINT32_MAX))
                {
                    chan->id = to;
                }

                if (type == resp_type &&
                    fromver == inst.inbound_version &&
                    tover == 0 &&
                    from == ent &&
                    to == chan->id &&
                    nonce == expected_nonce)
                {
                    up >> response;
                    up.leftovers(resp_msg);
                    *resp_stat = static_cast<hyperdex::network_returncode>(response);
                    return SUCCESS;
                }

                if (nonce == expected_nonce)
                {
                    return SERVERERROR;
                }
            }
            catch (po6::error& e)
            {
                channels.erase(inst);
                return DISCONNECT;
            }
            catch (std::out_of_range& e)
            {
                channels.erase(inst);
                return DISCONNECT;
            }
        }
    }
}

hyperclient::status
hyperclient :: client :: priv :: reqrep(const std::string& space,
                                        const e::buffer& key,
                                        hyperdex::network_msgtype req_type,
                                        hyperdex::network_msgtype resp_type,
                                        const e::buffer& msg,
                                        hyperdex::network_returncode* resp_status,
                                        e::buffer* resp_msg)
{
    std::map<std::string, hyperdex::spaceid>::const_iterator sai;

    // Map the string description to a number.
    if ((sai = config.space_assignment().find(space)) == config.space_assignment().end())
    {
        return NOTASPACE;
    }

    // Create the most restrictive region possible for this point.
    uint32_t spacenum = sai->second.space;
    uint64_t hash = CityHash64(static_cast<const char*>(key.get()), key.size());
    hyperdex::regionid r(spacenum, /*subspace*/ 0, /*prefix*/ 64, /*mask*/ hash);

    // Figure out who to talk with.
    hyperdex::entityid dst_ent;
    hyperdex::instance dst_inst;

    if (!find_entity(r, &dst_ent, &dst_inst))
    {
        return CONNECTFAIL;
    }

    e::intrusive_ptr<channel> chan = channels[dst_inst];

    if (!chan)
    {
        try
        {
            channels[dst_inst] = chan = new channel(dst_inst);
        }
        catch (po6::error& e)
        {
            return CONNECTFAIL;
        }
    }

    uint32_t nonce = chan->nonce;
    ++chan->nonce;
    status stat = send(chan, dst_inst, dst_ent, req_type, nonce, msg);

    if (stat != SUCCESS)
    {
        return stat;
    }

    return recv(chan, dst_inst, dst_ent, resp_type, nonce, resp_status, resp_msg);
}

hyperclient :: client :: priv :: channel :: channel(const hyperdex::instance& inst)
    : soc(inst.inbound.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , nonce(1)
    , id(UINT32_MAX)
    , m_ref(0)
{
    soc.connect(inst.inbound);
}

hyperclient :: client :: search_results :: search_results()
    : p(new priv())
{
}

hyperclient :: client :: search_results :: ~search_results()
                                           throw ()
{
}

bool
hyperclient :: client :: search_results :: valid()
{
    return p->valid;
}

hyperclient::status
hyperclient :: client :: search_results :: next()
{
    return p->next();
}

const e::buffer&
hyperclient :: client :: search_results :: key()
{
    return p->key;
}

const std::vector<e::buffer>&
hyperclient :: client :: search_results :: value()
{
    return p->value;
}

hyperclient :: client :: search_results :: priv :: priv()
    : c(NULL)
    , hosts()
    , chansubset()
    , nonces()
    , pfds()
    , pfds_idx()
    , valid(false)
    , key()
    , value()
{
}

hyperclient :: client :: search_results :: priv :: priv(client::priv* _c,
                                                        std::map<hyperdex::entityid, hyperdex::instance>* _h,
                                                        std::map<hyperdex::instance, channel_ptr>* _cs,
                                                        const hyperdex::search& s)
    : c(_c)
    , hosts()
    , chansubset()
    , nonces()
    , pfds()
    , pfds_idx()
    , valid(true)
    , key()
    , value()
{
    hosts.swap(*_h);
    chansubset.swap(*_cs);

    e::buffer msg;
    e::packer pack(msg.pack());
    pack << s;

    for (std::map<hyperdex::entityid, hyperdex::instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        channel_ptr chan = chansubset[h->second];
        assert(chan);
        uint32_t nonce = nonces[h->first] = chan->nonce;
        ++chan->nonce;
        c->send(chan, h->second, h->first, hyperdex::REQ_SEARCH_START, nonce, msg);
    }

    pfds.push_back(c->cl.pfd());
    pfds_idx.push_back(hyperdex::instance());

    for (std::map<hyperdex::instance, channel_ptr>::iterator i = chansubset.begin(); i != chansubset.end(); ++i)
    {
        pfds.push_back(pollfd());
        pfds.back().fd = i->second->soc.get();
        pfds.back().events = POLLIN;
        pfds.back().revents = 0;
        pfds_idx.push_back(i->first);
    }
}

hyperclient :: client :: search_results :: priv :: ~priv()
                                                   throw ()
{
    e::buffer msg;

    for (std::map<hyperdex::entityid, hyperdex::instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        channel_ptr chan = chansubset[h->second];

        if (!chan)
        {
            chansubset.erase(h->second);
            continue;
        }

        c->send(chan, h->second, h->first, hyperdex::REQ_SEARCH_STOP, nonces[h->first], msg);
    }
}

hyperclient::status
hyperclient :: client :: search_results :: priv :: next()
{
    while (valid && !hosts.empty())
    {
        if (!c->cl.connected())
        {
            switch (c->cl.connect())
            {
                case hyperdex::coordinatorlink::SUCCESS:
                    break;
                case hyperdex::coordinatorlink::CONNECTFAIL:
                case hyperdex::coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case hyperdex::coordinatorlink::SHUTDOWN:
                case hyperdex::coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }
        }

        pfds[0] = c->cl.pfd();

        if (poll(&pfds.front(), pfds.size(), -1) < 0)
        {
            return LOGICERROR;
        }

        if (pfds[0].revents != 0)
        {
            switch (c->cl.loop(1, 0))
            {
                case hyperdex::coordinatorlink::SUCCESS:
                    break;
                case hyperdex::coordinatorlink::CONNECTFAIL:
                case hyperdex::coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case hyperdex::coordinatorlink::SHUTDOWN:
                case hyperdex::coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }

            if (c->cl.unacknowledged())
            {
                c->config = c->cl.config();
                c->cl.acknowledge();
                bool fail = false;

                for (std::map<hyperdex::entityid, hyperdex::instance>::iterator host = hosts.begin();
                        host != hosts.end(); ++host)
                {
                    if (c->config.lookup(host->first) != host->second)
                    {
                        disconnect(host->second);
                        fail = true;
                    }
                }

                if (fail)
                {
                    return RECONFIGURE;
                }

                continue;
            }
        }

        for (size_t i = 1; i < pfds.size(); ++i)
        {
            if (pfds[i].fd == -1)
            {
                continue;
            }

            hyperdex::instance inst = pfds_idx[i];
            priv::channel_ptr chan = chansubset[inst];

            if (!chan)
            {
                disconnect(inst);
                pfds[i].fd = -1;
                continue;
            }

            if ((pfds[i].events & POLLHUP))
            {
                disconnect(inst, chan);
                return DISCONNECT;
            }

            if ((pfds[i].events & POLLIN))
            {
                try
                {
                    uint32_t size;

                    if (chan->soc.recv(&size, 4, MSG_DONTWAIT|MSG_PEEK) != 4)
                    {
                        continue;
                    }

                    size = be32toh(size);
                    size += sizeof(uint32_t);
                    e::buffer msg;

                    if (xread(&chan->soc, &msg, size) < size)
                    {
                        disconnect(inst, chan);
                        return DISCONNECT;
                    }

                    e::buffer new_key;
                    std::vector<e::buffer> new_value;
                    uint32_t nop;
                    uint8_t msg_type;
                    uint16_t fromver;
                    uint16_t tover;
                    hyperdex::entityid from;
                    hyperdex::entityid to;
                    uint32_t nonce;
                    uint64_t count;
                    e::unpacker up(msg.unpack());
                    up >> nop >> msg_type >> fromver >> tover >> from >> to
                       >> nonce;
                    hyperdex::network_msgtype type = static_cast<hyperdex::network_msgtype>(msg_type);
                    uint32_t expected_nonce = nonces[from];

                    if (chan->id == hyperdex::entityid(UINT32_MAX))
                    {
                        chan->id = to;
                    }

                    std::map<hyperdex::entityid, hyperdex::instance>::iterator hi;

                    if ((type == hyperdex::RESP_SEARCH_ITEM || type == hyperdex::RESP_SEARCH_DONE) &&
                        fromver == inst.inbound_version &&
                        tover == 0 &&
                        (hi = hosts.find(from)) != hosts.end() &&
                        hi->second == inst &&
                        to == chan->id &&
                        nonce == expected_nonce)
                    {
                        if (type == hyperdex::RESP_SEARCH_ITEM)
                        {
                            up >> count >> new_key >> new_value;
                            key.swap(new_key);
                            value.swap(new_value);
                            e::buffer empty;
                            c->send(chan, inst, from, hyperdex::REQ_SEARCH_NEXT, nonce, empty);
                            return SUCCESS;
                        }
                        else
                        {
                            complete(from);
                            continue;
                        }
                    }

                    if (nonce == expected_nonce)
                    {
                        disconnect(inst, chan);
                        return SERVERERROR;
                    }
                }
                catch (po6::error& e)
                {
                    if (e != EAGAIN && e != EWOULDBLOCK && e != EINTR)
                    {
                        disconnect(inst, chan);
                        return DISCONNECT;
                    }
                }
                catch (std::out_of_range& e)
                {
                    disconnect(inst, chan);
                    return DISCONNECT;
                }
            }
        }
    }

    valid = false;
    return SUCCESS;
}

void
hyperclient :: client :: search_results :: priv :: complete(const hyperdex::entityid& ent)
{
    std::map<hyperdex::entityid, hyperdex::instance>::iterator host;
    host = hosts.find(ent);

    if (host == hosts.end())
    {
        return;
    }

    hyperdex::instance inst = host->second;
    bool others = false;
    hosts.erase(host);
    nonces.erase(ent);

    for (host = hosts.begin(); host != hosts.end(); ++host)
    {
        if (host->second == inst)
        {
            others = true;
        }
    }

    if (!others)
    {
        disconnect(inst);
    }
}

void
hyperclient :: client :: search_results :: priv :: disconnect(const hyperdex::instance& inst)
{
    std::set<hyperdex::entityid> ents;

    for (std::map<hyperdex::entityid, hyperdex::instance>::iterator host = hosts.begin();
            host != hosts.end(); ++host)
    {
        if (host->second == inst)
        {
            ents.insert(host->first);
        }
    }

    for (std::set<hyperdex::entityid>::iterator e = ents.begin(); e != ents.end(); ++e)
    {
        hosts.erase(*e);
        nonces.erase(*e);
    }

    chansubset.erase(inst);
    assert(pfds.size() == pfds_idx.size());

    for (size_t i = 0; i < pfds.size(); ++i)
    {
        if (pfds_idx[i] == inst)
        {
            pfds[i].fd = -1;
        }
    }
}

void
hyperclient :: client :: search_results :: priv :: disconnect(const hyperdex::instance& inst,
                                                              channel_ptr chan)
{
    disconnect(inst);
    assert(pfds.size() == pfds_idx.size());

    for (size_t i = 0; i < pfds.size(); ++i)
    {
        if (pfds[i].fd == chan->soc.get())
        {
            pfds[i].fd = -1;
        }
    }

    chan->soc.shutdown(SHUT_RDWR);
    chan->soc.close();
    c->channels.erase(inst);
}
