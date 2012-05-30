// Copyright (c) 2012, Cornell University
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

// HyperDex
#include "hyperdex/hyperdex/configuration_parser.h"

hyperdex :: configuration_parser :: configuration_parser()
    : m_config_text("")
    , m_version(0)
    , m_hosts()
    , m_space_assignment()
    , m_spaces()
    , m_subspaces()
    , m_repl_attrs()
    , m_disk_attrs()
    , m_regions()
    , m_entities()
    , m_transfers()
    , m_quiesce(false)
    , m_quiesce_state_id("")
    , m_shutdown(false)
{
}

hyperdex :: configuration_parser :: ~configuration_parser() throw ()
{
}

hyperdex::configuration
hyperdex :: configuration_parser :: generate()
{
    std::vector<instance> hosts;
    hosts.reserve(m_hosts.size());

    for (std::map<uint64_t, instance>::const_iterator ci = m_hosts.begin();
            ci != m_hosts.end(); ++ci)
    {
        hosts.push_back(ci->second);
    }

    std::map<spaceid, uint16_t> space_sizes;

    for (std::set<subspaceid>::const_iterator ci = m_subspaces.begin();
            ci != m_subspaces.end(); ++ci)
    {
        ++space_sizes[ci->get_space()];
    }

    std::map<subspaceid, hyperspacehashing::prefix::hasher> repl_hashers;
    std::map<subspaceid, hyperspacehashing::mask::hasher> disk_hashers;
    std::map<subspaceid, std::vector<bool> >::const_iterator ri;
    std::map<subspaceid, std::vector<bool> >::const_iterator di;

    for (ri = m_repl_attrs.begin(); ri != m_repl_attrs.end(); ++ri)
    {
        hyperspacehashing::prefix::hasher h(attrs_to_hashfuncs(ri->first, ri->second));
        repl_hashers.insert(std::make_pair(ri->first, h));
    }

    for (di = m_disk_attrs.begin(); di != m_disk_attrs.end(); ++di)
    {
        hyperspacehashing::mask::hasher h(attrs_to_hashfuncs(di->first, di->second));
        disk_hashers.insert(std::make_pair(di->first, h));
    }

    return configuration(m_config_text, m_version, hosts, m_space_assignment, m_spaces, space_sizes,
                         m_entities, repl_hashers, disk_hashers, m_transfers,
                         m_quiesce, m_quiesce_state_id, m_shutdown);
}

#define _CONCAT(x,y) x ## y
#define CONCAT(x,y) _CONCAT(x,y)
#define ABORT_ON_ERROR(X) \
    do \
    { \
        error CONCAT(_anonymous_, __LINE__) = (X); \
        if (CONCAT(_anonymous_, __LINE__) != CP_SUCCESS) \
            return CONCAT(_anonymous_, __LINE__); \
    } while (0)


hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse(const std::string& config)
{
    *this = configuration_parser();
    m_config_text = config;
    std::vector<char> v;
    v.resize(config.size() + 1);
    memmove(&v.front(), config.c_str(), config.size() + 1);
    char* start = &v.front();
    char* eol = strchr(start, '\n');

    while (eol)
    {
        if (strncmp("version ", start, 8) == 0)
        {
            ABORT_ON_ERROR(parse_version(start, eol));
        }
        else if (strncmp("host ", start, 5) == 0)
        {
            ABORT_ON_ERROR(parse_host(start, eol));
        }
        else if (strncmp("space ", start, 6) == 0)
        {
            ABORT_ON_ERROR(parse_space(start, eol));
        }
        else if (strncmp("subspace ", start, 9) == 0)
        {
            ABORT_ON_ERROR(parse_subspace(start, eol));
        }
        else if (strncmp("region ", start, 7) == 0)
        {
            ABORT_ON_ERROR(parse_region(start, eol));
        }
        else if (strncmp("transfer ", start, 9) == 0)
        {
            ABORT_ON_ERROR(parse_transfer(start, eol));
        }
        else if (strncmp("quiesce ", start, 8) == 0)
        {
            ABORT_ON_ERROR(parse_quiesce(start, eol));
        }
        // shutdown has no space after (no args)
        else if (strncmp("shutdown", start, 8) == 0)
        {
            ABORT_ON_ERROR(parse_shutdown(start, eol));
        }
        else
        {
            return CP_UNKNOWN_CMD;
        }

        start = eol + 1;
        eol = strchr(start, '\n');
    }

    if (!start || *start != '\0')
    {
        return CP_EXCESS_DATA;
    }

    return CP_SUCCESS;
}

#define SKIP_WHITESPACE(start, eol) \
    while (start < eol && isspace(*start)) ++start
#define SKIP_TO_WHITESPACE(start, eol) \
    while (start < eol && !isspace(*start)) ++start

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_version(char* start,
                                                  char* const eol)
{
    char* end;
    uint64_t version;

    // Skip "version "
    start += 8;

    // Pull out the version number
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint64_t(start, end, &version));
    start = end + 1;

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    if (m_version != 0 && m_version != version)
    {
        return CP_DUPE_VERSION;
    }

    m_version = version;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_host(char* start,
                                               char* const eol)
{
    char* end;
    uint64_t id;
    po6::net::ipaddr ip;
    uint16_t iport;
    uint16_t iver;
    uint16_t oport;
    uint16_t over;

    // Skip "host "
    start += 5;

    // Pull out the host's numeric id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint64_t(start, end, &id));
    start = end + 1;

    // Pull out the ip address
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_ip(start, end, &ip));
    start = end + 1;

    // Pull out the incoming port number
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &iport));
    start = end + 1;

    // Pull out the incoming epoch id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &iver));
    start = end + 1;

    // Pull out the outgoing port number
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &oport));
    start = end + 1;

    // Pull out the outgoing epoch id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &over));

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    if (m_hosts.find(id) != m_hosts.end())
    {
        return CP_DUPE_HOST;
    }

    m_hosts[id] = instance(ip, iport, iver, oport, over);
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_space(char* start,
                                                char* const eol)
{
    char* end;
    std::string name;
    uint32_t id;
    std::vector<attribute> attributes;

    // Skip "space "
    start += 6;

    // Pull out the space's name
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    name = std::string(start, end);
    start = end + 1;

    // Pull out the spaces's numeric id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint32_t(start, end, &id));
    start = end + 1;

    while (start < eol)
    {
        hyperdatatype t;

        SKIP_WHITESPACE(start, eol);
        end = start;
        SKIP_TO_WHITESPACE(end, eol);
        *end = '\0';
        std::string dim(start, end);
        start = end + 1;

        SKIP_WHITESPACE(start, eol);
        end = start;
        SKIP_TO_WHITESPACE(end, eol);
        *end = '\0';
        ABORT_ON_ERROR(extract_datatype(start, end, &t));
        start = end + 1;

        for (std::vector<attribute>::const_iterator a = attributes.begin();
                a != attributes.end(); ++a)
        {
            if (a->name == dim)
            {
                return CP_DUPE_ATTR;
            }
        }

        attributes.push_back(attribute(dim, t));
    }

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    if (m_space_assignment.find(name) != m_space_assignment.end() ||
            m_spaces.find(spaceid(id)) != m_spaces.end())
    {
        return CP_DUPE_SPACE;
    }

    m_space_assignment[name] = spaceid(id);
    m_spaces[spaceid(id)] = attributes;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_subspace(char* start,
                                                   char* const eol)
{
    char* end;
    uint32_t space;
    uint16_t subspace;
    std::vector<std::string> attributes;

    // Skip "subspace "
    start += 9;

    // Pull out the space id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint32_t(start, end, &space));
    start = end + 1;

    // Pull out the subspace id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &subspace));
    start = end + 1;

    std::map<spaceid, std::vector<attribute> >::const_iterator si;

    if ((si = m_spaces.find(spaceid(space))) == m_spaces.end())
    {
        return CP_MISSING_SPACE;
    }

    std::vector<bool> repl_attrs(si->second.size(), false);
    std::vector<bool> disk_attrs(si->second.size(), false);

    for (size_t i = 0; i < si->second.size(); ++i)
    {
        bool repl;
        bool disk;

        SKIP_WHITESPACE(start, eol);
        end = start;
        SKIP_TO_WHITESPACE(end, eol);
        *end = '\0';
        ABORT_ON_ERROR(extract_bool(start, end, &repl));
        start = end + 1;

        SKIP_WHITESPACE(start, eol);
        end = start;
        SKIP_TO_WHITESPACE(end, eol);
        *end = '\0';
        ABORT_ON_ERROR(extract_bool(start, end, &disk));
        start = end + 1;

        if (subspace == 0 && i > 0 && repl)
        {
            return CP_BAD_ATTR_CHOICE;
        }

        if (subspace == 0 && i == 0 && !repl)
        {
            return CP_BAD_ATTR_CHOICE;
        }

        if (repl || disk)
        {
            switch (si->second[i].type)
            {
                // Searchable types
                case HYPERDATATYPE_STRING:
                case HYPERDATATYPE_INT64:
                    break;
                // Types which we cannot hash for searching
                case HYPERDATATYPE_LIST_STRING:
                case HYPERDATATYPE_LIST_INT64:
                case HYPERDATATYPE_SET_STRING:
                case HYPERDATATYPE_SET_INT64:
                case HYPERDATATYPE_MAP_STRING_STRING:
                case HYPERDATATYPE_MAP_STRING_INT64:
                case HYPERDATATYPE_MAP_INT64_STRING:
                case HYPERDATATYPE_MAP_INT64_INT64:
                    return CP_ATTR_NOT_SEARCHABLE;
                case HYPERDATATYPE_LIST_GENERIC:
                case HYPERDATATYPE_SET_GENERIC:
                case HYPERDATATYPE_MAP_GENERIC:
                case HYPERDATATYPE_MAP_STRING_KEYONLY:
                case HYPERDATATYPE_MAP_INT64_KEYONLY:
                case HYPERDATATYPE_GARBAGE:
                default:
                    abort();
            }
        }

        repl_attrs[i] = repl;
        disk_attrs[i] = disk;
    }

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    if (subspace > 0 &&
        m_subspaces.find(subspaceid(space, subspace - 1)) == m_subspaces.end())
    {
        return CP_OUTOFORDER_SUBSPACE;
    }

    if (m_subspaces.find(subspaceid(space, subspace)) != m_subspaces.end())
    {
        return CP_DUPE_SUBSPACE;
    }

    m_subspaces.insert(subspaceid(space, subspace));
    m_repl_attrs[subspaceid(space, subspace)] = repl_attrs;
    m_disk_attrs[subspaceid(space, subspace)] = disk_attrs;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_region(char* start,
                                                 char* const eol)
{
    char* end;
    uint32_t space;
    uint16_t subspace;
    uint8_t prefix;
    uint64_t mask;

    // Skip "region "
    start += 7;

    // Pull out the space id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint32_t(start, end, &space));
    start = end + 1;

    // Pull out the subspace id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &subspace));
    start = end + 1;

    // Pull out the prefix
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint8_t(start, end, &prefix));
    start = end + 1;

    // Pull out the mask
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint64_t(start, end, &mask));
    start = end + 1;

    if (m_subspaces.find(subspaceid(space, subspace)) == m_subspaces.end())
    {
        return CP_MISSING_SUBSPACE;
    }

    if (m_regions.find(regionid(space, subspace, prefix, mask)) != m_regions.end())
    {
        return CP_DUPE_REGION;
    }

    m_regions.insert(regionid(space, subspace, prefix, mask));
    uint8_t number = 0;

    while (start < eol)
    {
        uint64_t id;
        SKIP_WHITESPACE(start, eol);
        end = start;
        SKIP_TO_WHITESPACE(end, eol);
        *end = '\0';
        ABORT_ON_ERROR(extract_uint64_t(start, end, &id));
        start = end + 1;

        if (m_entities.find(entityid(space, subspace, prefix, mask, number)) !=
                m_entities.end())
        {
            return CP_DUPE_ENTITY;
        }

        std::map<uint64_t, instance>::const_iterator host;

        if ((host = m_hosts.find(id)) == m_hosts.end())
        {
            return CP_UNKNOWN_HOST;
        }

        m_entities[entityid(space, subspace, prefix, mask, number)]
            = host->second;
        ++number;
    }

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_transfer(char* start,
                                                   char* const eol)
{
    char* end;
    uint16_t xfer_id;
    uint32_t space;
    uint16_t subspace;
    uint8_t prefix;
    uint64_t mask;
    uint64_t host_id;

    // Skip "transfer "
    start += 9;

    // Pull out the transfer id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &xfer_id));
    start = end + 1;

    // Pull out the space id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint32_t(start, end, &space));
    start = end + 1;

    // Pull out the subspace id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint16_t(start, end, &subspace));
    start = end + 1;

    // Pull out the prefix
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint8_t(start, end, &prefix));
    start = end + 1;

    // Pull out the mask
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint64_t(start, end, &mask));
    start = end + 1;

    // Pull out the host's numeric id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    ABORT_ON_ERROR(extract_uint64_t(start, end, &host_id));
    start = end + 1;

    regionid reg(space, subspace, prefix, mask);

    if (m_regions.find(reg) == m_regions.end())
    {
        return CP_MISSING_REGION;
    }

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    std::map<std::pair<instance, uint16_t>, hyperdex::regionid>::iterator it;

    for (it = m_transfers.begin(); it != m_transfers.end(); ++it)
    {
        if (it->first.second == xfer_id)
        {
            return CP_DUPE_XFER;
        }
    }

    std::map<uint64_t, instance>::iterator host;
    host = m_hosts.find(host_id);

    if (host == m_hosts.end())
    {
        return CP_UNKNOWN_HOST;
    }

    size_t count = 0;
    bool live = false;

    for (std::map<entityid, instance>::iterator ent = m_entities.lower_bound(entityid(reg, 0));
            ent != m_entities.upper_bound(entityid(reg, UINT8_MAX)); ++ent)
    {
        ++count;

        if (ent->second == host->second)
        {
            live = true;
        }
    }

    if (count < 1 + (live ? 1 : 0))
    {
        return CP_F_FAILURES;
    }

    m_transfers.insert(std::make_pair(std::make_pair(host->second, xfer_id), reg));
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error 
hyperdex :: configuration_parser :: parse_quiesce(char* start,
                                                  char* const eol)
{
    char* end;
    std::string quiesce_state_id;

    // Skip "quiesce "
    start += 8;

    // Pull out the quiesce state id
    SKIP_WHITESPACE(start, eol);
    end = start;
    SKIP_TO_WHITESPACE(end, eol);
    *end = '\0';
    quiesce_state_id = std::string(start, end);
    start = end + 1;

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    if (m_quiesce_state_id != "" && m_quiesce_state_id != quiesce_state_id)
    {
        return CP_DUPE_QUIESCE_STATE_ID;
    }

    m_quiesce = true;
    m_quiesce_state_id = quiesce_state_id;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error 
hyperdex :: configuration_parser :: parse_shutdown(char* start,
                                                   char* const eol)
{
    char* end;

    // Skip "shutdown" (no args = no following space)
    start += 8;
    
    // No args
    end = start;

    if (end != eol)
    {
        return CP_EXCESS_DATA;
    }

    m_shutdown = true;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_bool(char* start,
                                                 char* end,
                                                 bool* t)
{
    assert(start <= end);
    assert(*end == '\0');

    if (start == end)
    {
        return CP_MISSING;
    }

    if (strcmp(start, "true") == 0)
    {
        *t = true;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "false") == 0)
    {
        *t = false;
        return CP_SUCCESS;
    }
    else
    {
        return CP_BAD_BOOL;
    }
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_datatype(char* start,
                                                     char* end,
                                                     hyperdatatype* t)
{
    assert(start <= end);
    assert(*end == '\0');

    if (start == end)
    {
        return CP_MISSING;
    }

    if (strcmp(start, "string") == 0)
    {
        *t = HYPERDATATYPE_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "int64") == 0)
    {
        *t = HYPERDATATYPE_INT64;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "list(string)") == 0)
    {
        *t = HYPERDATATYPE_LIST_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "list(int64)") == 0)
    {
        *t = HYPERDATATYPE_LIST_INT64;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "set(string)") == 0)
    {
        *t = HYPERDATATYPE_SET_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "set(int64)") == 0)
    {
        *t = HYPERDATATYPE_SET_INT64;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(string,string)") == 0)
    {
        *t = HYPERDATATYPE_MAP_STRING_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(string,int64)") == 0)
    {
        *t = HYPERDATATYPE_MAP_STRING_INT64;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(int64,string)") == 0)
    {
        *t = HYPERDATATYPE_MAP_INT64_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(int64,int64)") == 0)
    {
        *t = HYPERDATATYPE_MAP_INT64_INT64;
        return CP_SUCCESS;
    }
    else
    {
        return CP_BAD_TYPE;
    }
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_ip(char* start,
                                               char* end,
                                               po6::net::ipaddr* ip)
{
    assert(start <= end);
    assert(*end == '\0');

    if (start == end)
    {
        return CP_MISSING;
    }

    try
    {
        *ip = po6::net::ipaddr(start);
    }
    catch (po6::error& e)
    {
        return CP_BAD_IP;
    }

    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_uint64_t(char* start,
                                                     char* end,
                                                     uint64_t* num)
{
    assert(start <= end);
    assert(*end == '\0');
    char* endptr;
    uint64_t ret = strtoull(start, &endptr, 0);

    if (start == end)
    {
        return CP_MISSING;
    }

    if (endptr != end)
    {
        return CP_BAD_UINT64;
    }

    *num = ret;
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_uint32_t(char* start,
                                                     char* end,
                                                     uint32_t* num)
{
    assert(start <= end);
    assert(*end == '\0');
    char* endptr;
    uint64_t ret = strtoull(start, &endptr, 0);

    if (start == end)
    {
        return CP_MISSING;
    }

    if (endptr != end || (ret & 0xffffffffULL) != ret)
    {
        return CP_BAD_UINT32;
    }

    *num = static_cast<uint32_t>(ret);
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_uint16_t(char* start,
                                                     char* end,
                                                     uint16_t* num)
{
    assert(start <= end);
    assert(*end == '\0');
    char* endptr;
    uint64_t ret = strtoull(start, &endptr, 0);

    if (start == end)
    {
        return CP_MISSING;
    }

    if (endptr != end || (ret & 0xffffULL) != ret)
    {
        return CP_BAD_UINT16;
    }

    *num = static_cast<uint16_t>(ret);
    return CP_SUCCESS;
}

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: extract_uint8_t(char* start,
                                                    char* end,
                                                    uint8_t* num)
{
    assert(start <= end);
    assert(*end == '\0');
    char* endptr;
    uint64_t ret = strtoull(start, &endptr, 0);

    if (start == end)
    {
        return CP_MISSING;
    }

    if (endptr != end || (ret & 0xffULL) != ret)
    {
        return CP_BAD_UINT8;
    }

    *num = static_cast<uint8_t>(ret);
    return CP_SUCCESS;
}

std::vector<hyperspacehashing::hash_t>
hyperdex :: configuration_parser :: attrs_to_hashfuncs(const subspaceid& ssi,
                                                       const std::vector<bool>& attrs)
{
    std::map<spaceid, std::vector<attribute> >::const_iterator si;
    si = m_spaces.find(ssi.get_space());
    assert(si != m_spaces.end());
    assert(si->second.size() == attrs.size());
    std::vector<hyperspacehashing::hash_t> hfuncs;
    hfuncs.reserve(attrs.size());

    for (size_t i = 0; i < attrs.size(); ++i)
    {
        if (attrs[i])
        {
            switch (si->second[i].type)
            {
                case HYPERDATATYPE_STRING:
                    hfuncs.push_back(hyperspacehashing::EQUALITY);
                    break;
                case HYPERDATATYPE_INT64:
                    hfuncs.push_back(hyperspacehashing::RANGE);
                    break;
                case HYPERDATATYPE_LIST_GENERIC:
                case HYPERDATATYPE_LIST_STRING:
                case HYPERDATATYPE_LIST_INT64:
                case HYPERDATATYPE_SET_GENERIC:
                case HYPERDATATYPE_SET_STRING:
                case HYPERDATATYPE_SET_INT64:
                case HYPERDATATYPE_MAP_GENERIC:
                case HYPERDATATYPE_MAP_STRING_KEYONLY:
                case HYPERDATATYPE_MAP_STRING_STRING:
                case HYPERDATATYPE_MAP_STRING_INT64:
                case HYPERDATATYPE_MAP_INT64_KEYONLY:
                case HYPERDATATYPE_MAP_INT64_STRING:
                case HYPERDATATYPE_MAP_INT64_INT64:
                case HYPERDATATYPE_GARBAGE:
                default:
                    abort();
            }
        }
        else
        {
            hfuncs.push_back(hyperspacehashing::NONE);
        }
    }

    return hfuncs;
}
