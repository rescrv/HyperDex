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
#include <hyperdex/physical.h>
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
        priv();
        priv(client::priv* c, const std::map<entityid, instance>& hosts, const hyperdex::search& s);
        ~priv() throw ();

    public:
        client::priv* c;
        std::map<entityid, instance> hosts;
        bool valid;
        e::buffer key;
        std::vector<e::buffer> value;

    private:
        priv(const priv&);

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

#if 0
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

    // XXX Map params into a hyperdex::search.
    hyperdex::search terms;

    // Create the most restrictive region possible for this point.
    uint32_t spacenum = sai->second.space;
    std::map<regionid, size_t> regions = p->config.regions();
    std::map<uint16_t, std::map<entityid, instance> > candidates;

    for (std::map<regionid, size_t>::iterator r = regions.begin();
            r != regions.end(); ++r)
    {
        if (r->first.space == spacenum) // XXX We don't do the HD trick here.  We're contacting every host.
        {
            entityid ent = p->config.headof(r->first);
            instance inst = p->config.lookup(ent);
            candidates[r->first.subspace][ent] = inst;
        }
    }

    bool set = false;
    std::map<entityid, instance> hosts;

    for (std::map<uint16_t, std::map<entityid, instance> >::iterator c = candidates.begin();
            c != candidates.end(); ++c)
    {
        if (c->second.size() < hosts.size() || !set)
        {
            hosts.swap(c->second);
            set = true;
        }
    }

    sr->p.reset(new search_results::priv(p.get(), hosts, terms));
    return sr->next();
}
#endif

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

#if 0
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
    while (p->valid && !p->hosts.empty())
    {
        po6::net::location loc;
        e::buffer packed;

        switch (p->c->phys.recv(&loc, &packed))
        {
            case physical::SUCCESS:
                break;
            case physical::CONNECTFAIL:
                return CONNECTFAIL;
            case physical::DISCONNECT:
                return DISCONNECT;
            case physical::QUEUED:
            case physical::SHUTDOWN:
            case physical::LOGICERROR:
            default:
                assert(0);
        }

        uint8_t msg_type;
        uint16_t fromver;
        uint16_t tover;
        entityid from;
        entityid to;
        uint32_t nonce;
        e::buffer msg;
        packed.unpack() >> msg_type >> fromver >> tover >> from >> to >> nonce >> msg;
        entityid dst_from;
        instance dst_inst;

        if (!p->c->find_entity(from.get_region(), &dst_from, &dst_inst))
        {
        }

        if (dst_inst.inbound != loc || dst_inst.inbound_version != fromver)
        {
            continue;
        }

        switch (static_cast<network_msgtype>(msg_type))
        {
            case RESP_SEARCH_ITEM:
                break;
            case RESP_SEARCH_DONE:
                p->hosts.erase(from);
                continue;
            case REQ_GET:
            case RESP_GET:
            case REQ_PUT:
            case RESP_PUT:
            case REQ_DEL:
            case RESP_DEL:
            case REQ_SEARCH_START:
            case REQ_SEARCH_NEXT:
            case REQ_SEARCH_STOP:
            case CHAIN_PUT:
            case CHAIN_DEL:
            case CHAIN_PENDING:
            case CHAIN_ACK:
            case XFER_MORE:
            case XFER_DATA:
            case XFER_DONE:
            case PACKET_NOP:
            default:
                continue;
        }

        uint64_t count;
        p->key.clear();
        p->value.clear();
        msg.unpack() >> count >> p->key >> p->value;
        p->valid = true;
        msg.clear();
        uint32_t newnonce = p->c->nonces[dst_inst] ++;
        msg.pack() << newnonce;
        p->c->send(from, dst_inst, REQ_SEARCH_NEXT, newnonce, msg);
        return SUCCESS;
    }

    p->valid = false;
    return SUCCESS;
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
    : c()
    , hosts()
    , valid(false)
    , key()
    , value()
{
}

hyperdex :: client :: search_results :: priv :: priv(client::priv* _c,
                                                     const std::map<entityid, instance>& _hosts,
                                                     const hyperdex::search& s)
    : c(_c)
    , hosts(_hosts)
    , valid(true)
    , key()
    , value()
{
    e::buffer msg;
    e::packer pack(msg.pack());
    pack << s;

    for (std::map<entityid, instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        uint32_t nonce = c->nonces[h->second] ++;
        c->send(h->first, h->second, REQ_SEARCH_START, nonce, msg);
    }
}

hyperdex :: client :: search_results :: priv :: ~priv()
                                                throw ()
{
    e::buffer msg;

    for (std::map<entityid, instance>::iterator h = hosts.begin(); h != hosts.end(); ++h)
    {
        uint32_t nonce = c->nonces[h->second] ++;
        c->send(h->first, h->second, REQ_SEARCH_STOP, nonce, msg);
    }
}
#endif
