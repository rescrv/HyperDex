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

// C
#include <stdint.h>

// Libev
#include <ev++.h>

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/buffer.h>
#include <hyperdex/city.h>
#include <hyperdex/client.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/instance.h>
#include <hyperdex/masterlink.h>
#include <hyperdex/physical.h>
#include <hyperdex/stream_no.h>

class client_install_mapping
{
    public:
        client_install_mapping(std::map<hyperdex::entityid, hyperdex::instance>* mapping,
                               std::map<std::string, hyperdex::spaceid>* space_assignment,
                               ev::async* wakeup)
            : m_mapping(mapping)
            , m_space_assignment(space_assignment)
            , m_wakeup(wakeup)
        {
        }

    public:
        void operator () (const hyperdex::configuration& config)
        {
            *m_mapping = config.entity_mapping();
            *m_space_assignment = config.space_assignment();
            m_wakeup->send();
        }

    private:
        std::map<hyperdex::entityid, hyperdex::instance>* m_mapping;
        std::map<std::string, hyperdex::spaceid>* m_space_assignment;
        ev::async* m_wakeup;
};

struct hyperdex :: client :: priv
{
    priv(po6::net::location coordinator)
        : dl()
        , wakeup(dl)
        , phys(dl, po6::net::ipaddr::ANY())
        , mapping()
        , space_assignment()
        , ml(coordinator, "client\n", client_install_mapping(&mapping, &space_assignment, &wakeup))
        , id(UINT32_MAX, 0, 0, 0, 0)
    {
        wakeup.set<priv, &priv::nil>(this);
        wakeup.start();
    }

    void nil(ev::async&, int) {}

    bool find_entity(const regionid& r, entityid* ent, instance* inst)
    {
        bool found = false;

        for (std::map<entityid, instance>::iterator e = mapping.begin();
                e != mapping.end(); ++e)
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

    bool send(const entityid& ent, const instance& inst, stream_no::stream_no_t act, const e::buffer& msg)
    {
        const uint8_t msg_type = act;
        const uint16_t fromver = 0;
        const uint16_t tover = inst.inbound_version;
        const entityid from(UINT32_MAX, 0, 0, 0, 0);
        const entityid& to(ent);
        e::buffer packed;

        packed.pack() << msg_type << fromver << tover << from << to << msg;
        phys.send(inst.inbound, packed);
        return true;
    }

    bool recv(entityid* ent, stream_no::stream_no_t* act, e::buffer* msg)
    {
        while (!phys.pending())
        {
            dl.loop(EVLOOP_ONESHOT);
        }

        po6::net::location loc;
        e::buffer packed;

        if (phys.recv(&loc, &packed))
        {
            uint8_t msg_type;
            uint16_t fromver;
            uint16_t tover;
            entityid from;
            entityid to;

            packed.unpack() >> msg_type >> fromver >> tover >> from >> to >> *msg;
            *ent = from;
            *act = static_cast<stream_no::stream_no_t>(msg_type);
            return true;
        }
        else
        {
            return false;
        }
    }

    ev::dynamic_loop dl;
    ev::async wakeup;
    hyperdex::physical phys;
    std::map<entityid, instance> mapping;
    std::map<std::string, spaceid> space_assignment;
    hyperdex::masterlink ml;
    entityid id;
};

hyperdex :: client :: client(po6::net::location coordinator)
    : p(new priv(coordinator))
{
    while (p->mapping.empty())
    {
        p->dl.loop(EVLOOP_ONESHOT);
    }
}

hyperdex :: result_t
hyperdex :: client :: get(const std::string& space,
                          const e::buffer& key,
                          std::vector<e::buffer>* value)
{
    std::map<std::string, spaceid>::iterator sai;

    // Map the string description to a number.
    if ((sai = p->space_assignment.find(space)) == p->space_assignment.end())
    {
        LOG(INFO) << "space name does not translate to space number.";
        return INVALID;
    }

    // Create the most restrictive region possible for this point.
    uint32_t spacenum = sai->second.space;
    uint64_t hash = CityHash64(static_cast<const char*>(key.get()), key.size());
    regionid r(spacenum, /*subspace*/ 0, /*prefix*/ 64, /*mask*/ hash);

    // Figure out who to talk with.
    entityid dst_ent;
    instance dst_inst;

    if (!p->find_entity(r, &dst_ent, &dst_inst))
    {
        LOG(INFO) << "no entry matches the given point.";
        return INVALID;
    }

    // Send the get messasge
    e::buffer msg;
    uint32_t nonce = 0;
    msg.pack() << nonce << key;

    if (!p->send(dst_ent, dst_inst, stream_no::GET, msg))
    {
        LOG(INFO) << "could not send message.";
        return ERROR;
    }

    // Receive the message
    stream_no::stream_no_t type;
    entityid from;
    msg.clear();

    if (p->recv(&from, &type, &msg))
    {
        if (type == stream_no::RESULT)
        {
            uint8_t result;
            msg.unpack() >> nonce >> result >> *value;
            return static_cast<result_t>(result);
        }

        LOG(INFO) << "received non-result message.";
        return ERROR;
    }

    LOG(INFO) << "could not recv message.";
    return ERROR;
}

hyperdex :: result_t
hyperdex :: client :: put(const std::string& space,
                          const e::buffer& key,
                          const std::vector<e::buffer>& value)
{
    std::map<std::string, spaceid>::iterator sai;

    // Map the string description to a number.
    if ((sai = p->space_assignment.find(space)) == p->space_assignment.end())
    {
        LOG(INFO) << "space name does not translate to space number.";
        return INVALID;
    }

    // Create the most restrictive region possible for this point.
    uint32_t spacenum = sai->second.space;
    uint64_t hash = CityHash64(static_cast<const char*>(key.get()), key.size());
    regionid r(spacenum, /*subspace*/ 0, /*prefix*/ 64, /*mask*/ hash);

    // Figure out who to talk with.
    entityid dst_ent;
    instance dst_inst;

    if (!p->find_entity(r, &dst_ent, &dst_inst))
    {
        LOG(INFO) << "no entry matches the given point.";
        return INVALID;
    }

    // Send the put messasge
    e::buffer msg;
    uint32_t nonce = 0;
    uint32_t size = key.size();
    msg.pack() << nonce << size << key << value;

    if (!p->send(dst_ent, dst_inst, stream_no::PUT, msg))
    {
        LOG(INFO) << "could not send message.";
        return ERROR;
    }

    // Receive the message
    stream_no::stream_no_t type;
    entityid from;
    msg.clear();

    if (p->recv(&from, &type, &msg))
    {
        if (type == stream_no::RESULT)
        {
            uint8_t result;
            msg.unpack() >> nonce >> result;
            return static_cast<result_t>(result);
        }

        LOG(INFO) << "received non-result message.";
        return ERROR;
    }

    LOG(INFO) << "could not recv message.";
    return ERROR;
}

hyperdex :: result_t
hyperdex :: client :: del(const std::string& space,
                          const e::buffer& key)
{
    // XXX
    return INVALID;
}
