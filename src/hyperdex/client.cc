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
#include <city/city.h>

// e
#include <e/intrusive_ptr.h>

// Coordination
#include <configuration/coordinatorlink.h>

// HyperDex
#include <hyperdex/hyperspace.h>
#include <hyperdex/ids.h>
#include <hyperdex/network_constants.h>
#include <hyperdex/search.h>

// HyperClient
#include <hyperdex/client.h>

class hyperdex :: client :: priv
{
    public:
        class channel;

    public:
        priv(const po6::net::location& coordinator);
        ~priv() throw ();

    public:
        bool find_entity(const regionid& r, entityid* ent, instance* inst);
        hyperdex::status send(e::intrusive_ptr<channel> chan,
                              const instance& inst,
                              const entityid& ent,
                              network_msgtype type,
                              uint32_t nonce,
                              const e::buffer& msg);
        hyperdex::status recv(e::intrusive_ptr<channel> chan,
                              const instance& inst,
                              const entityid& ent,
                              network_msgtype resp_type,
                              uint32_t expected_nonce,
                              network_returncode* resp_stat,
                              e::buffer* resp_msg);
        hyperdex::status reqrep(const std::string& space,
                                const e::buffer& key,
                                network_msgtype req_type,
                                network_msgtype resp_type,
                                const e::buffer& msg,
                                network_returncode* resp_status,
                                e::buffer* resp_msg);

    public:
        coordinatorlink cl;
        configuration config;
        std::map<instance, e::intrusive_ptr<channel> > channels;
};

class hyperdex :: client :: priv :: channel
{
    public:
        channel(const instance& inst);

    public:
        po6::net::socket soc;
        uint64_t nonce;
        entityid id;

    private:
        friend class e::intrusive_ptr<channel>;

    private:
        size_t m_ref;
};

class hyperdex::client::search_results::priv
{
    public:
        typedef e::intrusive_ptr<client::priv::channel> channel_ptr;

    public:
        priv();
        priv(client::priv* c,
             std::map<entityid, instance>* hosts,
             std::map<instance, channel_ptr>* chansubset,
             const hyperdex::search& s);
        ~priv() throw ();

    public:
        status next();

    public:
        client::priv* c;
        std::map<entityid, instance> hosts;
        std::map<instance, channel_ptr> chansubset;
        std::map<entityid, uint32_t> nonces;
        std::vector<pollfd> pfds;
        std::vector<instance> pfds_idx;
        bool valid;
        e::buffer key;
        std::vector<e::buffer> value;

    private:
        priv(const priv&);

    private:
        void remove(const instance& inst);
        void disconnect(const instance& inst, channel_ptr chan);

    private:
        priv& operator = (const priv&);
};

hyperdex :: client :: client(po6::net::location coordinator)
    : p(new priv(coordinator))
{
}

hyperdex::status
hyperdex :: client :: connect()
{
    switch (p->cl.connect())
    {
        case coordinatorlink::SUCCESS:
            break;
        case coordinatorlink::CONNECTFAIL:
            return COORDFAIL;
        case coordinatorlink::DISCONNECT:
        case coordinatorlink::SHUTDOWN:
        case coordinatorlink::LOGICERROR:
        default:
            return LOGICERROR;
    }

    while (true)
    {
        switch (p->cl.loop(1, -1))
        {
            case coordinatorlink::SUCCESS:
                break;
            case coordinatorlink::CONNECTFAIL:
                return COORDFAIL;
            case coordinatorlink::DISCONNECT:
                return COORDFAIL;
            case coordinatorlink::SHUTDOWN:
            case coordinatorlink::LOGICERROR:
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

hyperdex::status
hyperdex :: client :: get(const std::string& space,
                          const e::buffer& key,
                          std::vector<e::buffer>* value)
{
    network_returncode resp_status;
    e::buffer resp_msg;
    hyperdex::status stat = p->reqrep(space, key, REQ_GET, RESP_GET, key, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case NET_SUCCESS:
                resp_msg.unpack() >> *value;
                return SUCCESS;
            case NET_NOTFOUND:
                return NOTFOUND;
            case NET_WRONGARITY:
                return WRONGARITY;
            case NET_NOTUS:
                return LOGICERROR;
            case NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperdex::status
hyperdex :: client :: put(const std::string& space,
                          const e::buffer& key,
                          const std::vector<e::buffer>& value)
{
    e::buffer msg;
    msg.pack() << key << value;
    network_returncode resp_status;
    e::buffer resp_msg;
    hyperdex::status stat = p->reqrep(space, key, REQ_PUT, RESP_PUT, msg, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case NET_SUCCESS:
                return SUCCESS;
            case NET_NOTFOUND:
                return NOTFOUND;
            case NET_WRONGARITY:
                return WRONGARITY;
            case NET_NOTUS:
                return LOGICERROR;
            case NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperdex::status
hyperdex :: client :: del(const std::string& space,
                          const e::buffer& key)
{
    network_returncode resp_status;
    e::buffer resp_msg;
    hyperdex::status stat = p->reqrep(space, key, REQ_DEL, RESP_DEL, key, &resp_status, &resp_msg);

    if (stat == SUCCESS)
    {
        switch (resp_status)
        {
            case NET_SUCCESS:
                return SUCCESS;
            case NET_NOTFOUND:
                return NOTFOUND;
            case NET_WRONGARITY:
                return WRONGARITY;
            case NET_NOTUS:
                return LOGICERROR;
            case NET_SERVERERROR:
                return SERVERERROR;
            default:
                return SERVERERROR;
        }
    }

    return stat;
}

hyperdex::status
hyperdex :: client :: search(const std::string& space,
                             const std::map<std::string, e::buffer>& params,
                             search_results* sr)
{
    std::map<std::string, spaceid>::const_iterator sai;

    // Map the string description to a number.
    if ((sai = p->config.space_assignment().find(space)) == p->config.space_assignment().end())
    {
        return NOTASPACE;
    }

    uint32_t spacenum = sai->second.space;
    std::map<spaceid, std::vector<std::string> >::const_iterator si;

    if ((si = p->config.spaces().find(spaceid(spacenum))) == p->config.spaces().end())
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

    typedef std::map<uint16_t, std::map<entityid, instance> > candidates_map;
    std::map<regionid, size_t> regions = p->config.regions();
    std::map<regionid, size_t>::iterator reg_current;
    std::map<regionid, size_t>::iterator reg_end;
    candidates_map candidates;
    reg_current = regions.lower_bound(regionid(spacenum, 1, 0, 0));
    reg_end = regions.upper_bound(regionid(spacenum, UINT16_MAX, UINT8_MAX, UINT64_MAX));
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

        uint64_t pmask = prefixmask(reg_current->first.prefix);

        if ((reg_current->first.mask & mask) == (point & pmask))
        {
            entityid ent = p->config.headof(reg_current->first);
            instance inst = p->config.lookup(ent);
            candidates[reg_current->first.subspace][ent] = inst;
        }
    }

    bool set = false;
    std::map<entityid, instance> hosts;

    for (candidates_map::iterator c = candidates.begin(); c != candidates.end(); ++c)
    {
        if (c->second.size() < hosts.size() || !set)
        {
            hosts.swap(c->second);
            set = true;
        }
    }

    status ret = SUCCESS;
    std::map<instance, e::intrusive_ptr<priv::channel> > chansubset;

    for (std::map<entityid, instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
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

        chansubset[h->second] = chan;
    }

    sr->p.reset(new search_results::priv(p.get(), &hosts, &chansubset, terms));
    return sr->next();
}

hyperdex :: client :: priv :: priv(const po6::net::location& coordinator)
    : cl(coordinator, "client")
    , config()
    , channels()
{
}

hyperdex :: client :: priv :: ~priv() throw ()
{
}

bool
hyperdex :: client :: priv :: find_entity(const regionid& r,
                                          entityid* ent,
                                          instance* inst)
{
    bool found = false;

    for (std::map<entityid, instance>::const_iterator e = config.entity_mapping().begin();
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

hyperdex::status
hyperdex :: client :: priv ::  send(e::intrusive_ptr<channel> chan,
                                    const instance& inst,
                                    const entityid& ent,
                                    network_msgtype type,
                                    uint32_t nonce,
                                    const e::buffer& msg)
{
    const uint8_t msg_type = static_cast<uint8_t>(type);
    const uint16_t fromver = 0;
    const uint16_t tover = inst.inbound_version;
    const entityid& from(chan->id);
    const entityid& to(ent);
    const uint32_t size = sizeof(uint32_t) + sizeof(msg_type) + sizeof(fromver)
                        + sizeof(tover) + entityid::SERIALIZEDSIZE * 2
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

hyperdex::status
hyperdex :: client :: priv :: recv(e::intrusive_ptr<channel> chan,
                                   const instance& inst,
                                   const entityid& ent,
                                   network_msgtype resp_type,
                                   uint32_t expected_nonce,
                                   network_returncode* resp_stat,
                                   e::buffer* resp_msg)
{
    e::buffer partial(4);

    while (true)
    {
        if (!cl.connected())
        {
            switch (cl.connect())
            {
                case coordinatorlink::SUCCESS:
                    break;
                case coordinatorlink::CONNECTFAIL:
                case coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case coordinatorlink::SHUTDOWN:
                case coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }
        }

        pollfd pfds[2];
        memmove(pfds, &cl.pfd(), sizeof(pollfd));
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
                case coordinatorlink::SUCCESS:
                    break;
                case coordinatorlink::CONNECTFAIL:
                case coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case coordinatorlink::SHUTDOWN:
                case coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }

            if (cl.unacknowledged())
            {
                config = cl.config();
                cl.acknowledge();

                if (cl.config().lookup(ent) != inst)
                {
                    if (!partial.empty())
                    {
                        channels.erase(inst);
                    }

                    return RECONFIGURE;
                }

                continue;
            }
        }

        if ((pfds[1].revents & POLLIN))
        {
            try
            {
                if (read(&chan->soc, &partial, 1024) <= 0)
                {
                    channels.erase(inst);
                    return DISCONNECT;
                }
            }
            catch (po6::error& e)
            {
                channels.erase(inst);
                return DISCONNECT;
            }

            if (partial.size() >= 4)
            {
                uint32_t size;
                partial.unpack() >> size;

                if (partial.size() > size + sizeof(uint32_t))
                {
                    channels.erase(inst);
                    return SERVERERROR;
                }
                else if (partial.size() < size + sizeof(uint32_t))
                {
                    continue;
                }
            }

            uint32_t nop;
            uint8_t msg_type;
            uint16_t fromver;
            uint16_t tover;
            entityid from;
            entityid to;
            uint32_t nonce;
            uint16_t response;
            e::unpacker up(partial.unpack());
            up >> nop >> msg_type >> fromver >> tover >> from >> to
               >> nonce >> response;
            network_msgtype type = static_cast<network_msgtype>(msg_type);
            up.leftovers(resp_msg);
            *resp_stat = static_cast<network_returncode>(response);

            if (type == resp_type &&
                fromver == inst.inbound_version &&
                tover == 0 &&
                from == ent &&
                (to == chan->id || chan->id == entityid(UINT32_MAX)) &&
                nonce == expected_nonce)
            {
                return SUCCESS;
            }

            return SERVERERROR;
        }
    }
}

hyperdex::status
hyperdex :: client :: priv :: reqrep(const std::string& space,
                                     const e::buffer& key,
                                     network_msgtype req_type,
                                     network_msgtype resp_type,
                                     const e::buffer& msg,
                                     network_returncode* resp_status,
                                     e::buffer* resp_msg)
{
    std::map<std::string, spaceid>::const_iterator sai;

    // Map the string description to a number.
    if ((sai = config.space_assignment().find(space)) == config.space_assignment().end())
    {
        return NOTASPACE;
    }

    // Create the most restrictive region possible for this point.
    uint32_t spacenum = sai->second.space;
    uint64_t hash = CityHash64(static_cast<const char*>(key.get()), key.size());
    regionid r(spacenum, /*subspace*/ 0, /*prefix*/ 64, /*mask*/ hash);

    // Figure out who to talk with.
    entityid dst_ent;
    instance dst_inst;

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

hyperdex :: client :: priv :: channel :: channel(const instance& inst)
    : soc(inst.inbound.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , nonce(0)
    , id(UINT32_MAX)
    , m_ref(0)
{
    soc.connect(inst.inbound);
}

hyperdex :: client :: search_results :: search_results()
    : p(new priv())
{
}

hyperdex :: client :: search_results :: ~search_results()
                                        throw ()
{
}

bool
hyperdex :: client :: search_results :: valid()
{
    return p->valid;
}

hyperdex::status
hyperdex :: client :: search_results :: next()
{
    return p->next();
}

const e::buffer&
hyperdex :: client :: search_results :: key()
{
    return p->key;
}

const std::vector<e::buffer>&
hyperdex :: client :: search_results :: value()
{
    return p->value;
}

hyperdex :: client :: search_results :: priv :: priv()
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

hyperdex :: client :: search_results :: priv :: priv(client::priv* _c,
                                                     std::map<entityid, instance>* _h,
                                                     std::map<instance, channel_ptr>* _cs,
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

    for (std::map<entityid, instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        channel_ptr chan = chansubset[h->second];
        assert(chan);
        uint32_t nonce = nonces[h->first] = chan->nonce;
        ++chan->nonce;
        c->send(chan, h->second, h->first, REQ_SEARCH_START, nonce, msg);
    }

    pfds.resize(chansubset.size() + 1);
    pfds_idx.resize(chansubset.size() + 1);
    memmove(&pfds[0], &c->cl.pfd(), sizeof(pollfd));
    size_t pos = 1;

    for (std::map<instance, channel_ptr>::iterator i = chansubset.begin(); i != chansubset.end(); ++i)
    {
        pfds[pos].fd = i->second->soc.get();
        pfds[pos].events = POLLIN;
        pfds[pos].revents = 0;
        pfds_idx[pos] = i->first;
        ++pos;
    }
}

hyperdex :: client :: search_results :: priv :: ~priv()
                                                throw ()
{
    e::buffer msg;

    for (std::map<entityid, instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        channel_ptr chan = chansubset[h->second];

        if (!chan)
        {
            continue;
        }

        c->send(chan, h->second, h->first, REQ_SEARCH_STOP, nonces[h->first], msg);
    }
}

hyperdex::status
hyperdex :: client :: search_results :: priv :: next()
{
    while (valid && !hosts.empty())
    {
        if (!c->cl.connected())
        {
            switch (c->cl.connect())
            {
                case coordinatorlink::SUCCESS:
                    break;
                case coordinatorlink::CONNECTFAIL:
                case coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case coordinatorlink::SHUTDOWN:
                case coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }
        }

        memmove(&pfds[0], &c->cl.pfd(), sizeof(pollfd));
        pfds[0].revents = 0;

        if (poll(&pfds.front(), pfds.size(), -1) < 0)
        {
            return LOGICERROR;
        }

        if (pfds[0].revents != 0)
        {
            switch (c->cl.loop(1, 0))
            {
                case coordinatorlink::SUCCESS:
                    break;
                case coordinatorlink::CONNECTFAIL:
                case coordinatorlink::DISCONNECT:
                    return COORDFAIL;
                case coordinatorlink::SHUTDOWN:
                case coordinatorlink::LOGICERROR:
                default:
                    return LOGICERROR;
            }

            if (c->cl.unacknowledged())
            {
                c->config = c->cl.config();
                c->cl.acknowledge();
                bool fail = false;

                for (std::map<entityid, instance>::iterator host = hosts.begin();
                        host != hosts.end(); ++host)
                {
                    if (c->config.lookup(host->first) != host->second)
                    {
                        entityid ent = host->first;
                        instance inst = host->second;
                        hosts.erase(ent);
                        chansubset.erase(inst);
                        nonces.erase(ent);
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

            instance inst = pfds_idx[i];
            priv::channel_ptr chan = chansubset[inst];

            if (!chan)
            {
                remove(inst);
                chansubset.erase(inst);
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

                    if (chan->soc.xread(&size, sizeof(size)) != sizeof(size))
                    {
                        disconnect(inst, chan);
                        return DISCONNECT;
                    }

                    size = be32toh(size);
                    e::buffer msg(size);

                    if (xread(&chan->soc, &msg, size) != size)
                    {
                        disconnect(inst, chan);
                        return DISCONNECT;
                    }

                    e::buffer new_key;
                    std::vector<e::buffer> new_value;
                    uint8_t msg_type;
                    uint16_t fromver;
                    uint16_t tover;
                    entityid from;
                    entityid to;
                    uint32_t nonce;
                    uint64_t count;
                    e::unpacker up(msg.unpack());
                    up >> msg_type >> fromver >> tover >> from >> to
                       >> nonce;
                    network_msgtype type = static_cast<network_msgtype>(msg_type);
                    uint32_t exp_nonce = nonces[from];

                    if ((type == RESP_SEARCH_ITEM || type == RESP_SEARCH_DONE) &&
                        fromver == inst.inbound_version &&
                        tover == 0 &&
                        hosts.find(from) != hosts.end() &&
                        hosts[from] == inst &&
                        (to == chan->id || chan->id == entityid(UINT32_MAX)) &&
                        nonce == exp_nonce)
                    {
                        if (type == RESP_SEARCH_ITEM)
                        {
                            e::buffer nop;
                            up >> count >> new_key >> new_value;
                            key.swap(new_key);
                            value.swap(new_value);
                            c->send(chan, inst, from, REQ_SEARCH_NEXT, nonce, nop);
                            return SUCCESS;
                        }
                        else
                        {
                            remove(inst);
                            continue;
                        }
                    }

                    disconnect(inst, chan);
                    return SERVERERROR;
                }
                catch (po6::error& e)
                {
                    disconnect(inst, chan);
                    return DISCONNECT;
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
hyperdex :: client :: search_results :: priv :: remove(const instance& inst)
{
    std::set<entityid> ents;

    for (std::map<entityid, instance>::iterator host = hosts.begin();
            host != hosts.end(); ++host)
    {
        if (host->second == inst)
        {
            ents.insert(host->first);
        }
    }

    for (std::set<entityid>::iterator e = ents.begin(); e != ents.end(); ++e)
    {
        hosts.erase(*e);
        nonces.erase(*e);
    }

    chansubset.erase(inst);

    for (size_t i = 0; i < pfds_idx.size(); ++i)
    {
        if (pfds_idx[i] == inst)
        {
            pfds[i].fd = -1;
            pfds[i].events = 0;
            pfds[i].revents = 0;
        }
    }
}

void
hyperdex :: client :: search_results :: priv :: disconnect(const instance& inst,
                                                           channel_ptr chan)
{
    remove(inst);
    chan->soc.close();
    c->channels.erase(inst);
}
