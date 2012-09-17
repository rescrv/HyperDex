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
    : m_config(NULL)
    , m_config_sz(0)
    , m_attributes(NULL)
    , m_attributes_sz(0)
    , m_schemas(NULL)
    , m_schemas_sz(0)
    , m_config_text("")
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
    reset();
}

hyperdex :: configuration_parser :: ~configuration_parser() throw ()
{
}

hyperdex::configuration
hyperdex :: configuration_parser :: generate()
{
    std::vector<std::pair<spaceid, schema*> > space_ids_to_schemas;

    // Create the "schema" objects that are needed for the attributes.
    size_t attrs_sz = 0;

    for (std::map<spaceid, std::vector<attribute> >::const_iterator ci = m_spaces.begin();
            ci != m_spaces.end(); ++ci)
    {
        attrs_sz += ci->second.size();
    }

    space_ids_to_schemas.resize(m_spaces.size());
    e::array_ptr<schema> schemas(new schema[m_spaces.size()]);
    size_t schemas_sz(m_spaces.size());
    e::array_ptr<attribute> attrs(new attribute[attrs_sz]);
    size_t schema_idx = 0;
    size_t attr_idx = 0;

    for (std::map<spaceid, std::vector<attribute> >::const_iterator ci = m_spaces.begin();
            ci != m_spaces.end(); ++ci)
    {
        space_ids_to_schemas[schema_idx].first = ci->first;
        space_ids_to_schemas[schema_idx].second = &schemas[schema_idx];
        schemas[schema_idx].attrs_sz = ci->second.size();
        schemas[schema_idx].attrs = &attrs[attr_idx];
        ++schema_idx;

        for (size_t i = 0; i < ci->second.size(); ++i)
        {
            attrs[attr_idx] = ci->second[i];
            ++attr_idx;
        }
    }

    // Do the rest of the generation
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

    return configuration(m_config, m_config_text.size() + 1, schemas, schemas_sz,
                         attrs, attrs_sz, space_ids_to_schemas,
                         m_config_text, m_version, hosts, m_space_assignment,
                         space_sizes, m_entities, repl_hashers, disk_hashers,
                         m_transfers, m_quiesce, m_quiesce_state_id, m_shutdown);
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
    reset();
    m_config_text = config;
    m_config_sz = config.size() + 1;
    m_config = new char[m_config_sz];
    memmove(m_config.get(), config.c_str(), m_config_sz);
    char* start = m_config.get();
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

        *eol = '\0';
        start = eol + 1;
        eol = strchr(start, '\n');
    }

    if (!start || *start != '\0')
    {
        return CP_EXCESS_DATA;
    }

    return CP_SUCCESS;
}

void
hyperdex :: configuration_parser :: reset()
{
    m_config = NULL;
    m_config_text = std::string();
    m_version = 0;
    m_hosts.clear();
    m_space_assignment.clear();
    m_spaces.clear();
    m_subspaces.clear();
    m_repl_attrs.clear();
    m_disk_attrs.clear();
    m_regions.clear();
    m_entities.clear();
    m_transfers.clear();
    m_quiesce = false;
    m_quiesce_state_id = std::string();
    m_shutdown = false;
}

#define SKIP_WHITESPACE(ptr, eol) \
    while (ptr < eol && isspace(*ptr)) ++ptr
#define SKIP_TO_WHITESPACE(ptr, eol) \
    while (ptr < eol && !isspace(*ptr)) ++ptr

#define PARSE_TOKEN(extract, store) \
    SKIP_WHITESPACE(start, eol); \
    end = start; \
    SKIP_TO_WHITESPACE(end, eol); \
    *end = '\0'; \
    ABORT_ON_ERROR(extract(start, end, &store))

hyperdex::configuration_parser::error
hyperdex :: configuration_parser :: parse_version(char* start,
                                                  char* const eol)
{
    char* end;
    uint64_t version;

    // Skip "version "
    start += 8;

    // Pull out the version number
    PARSE_TOKEN(extract_uint64_t, version);
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
    PARSE_TOKEN(extract_uint64_t, id);
    start = end + 1;
    // Pull out the ip address
    PARSE_TOKEN(extract_ip, ip);
    start = end + 1;
    // Pull out the incoming port number
    PARSE_TOKEN(extract_uint16_t, iport);
    start = end + 1;
    // Pull out the incoming epoch id
    PARSE_TOKEN(extract_uint16_t, iver);
    start = end + 1;
    // Pull out the outgoing port number
    PARSE_TOKEN(extract_uint16_t, oport);
    start = end + 1;
    // Pull out the outgoing epoch id
    PARSE_TOKEN(extract_uint16_t, over);

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
    char* name;
    uint32_t id;
    std::vector<attribute> attributes;

    // Skip "space "
    start += 6;

    // Pull out the space's name
    PARSE_TOKEN(extract_cstring, name);
    name = start;
    start = end + 1;
    // Pull out the spaces's numeric id
    PARSE_TOKEN(extract_uint32_t, id);
    start = end + 1;

    while (start < eol)
    {
        char* attr;
        hyperdatatype t;

        PARSE_TOKEN(extract_cstring, attr);
        start = end + 1;

        PARSE_TOKEN(extract_datatype, t);
        start = end + 1;

        for (std::vector<attribute>::const_iterator a = attributes.begin();
                a != attributes.end(); ++a)
        {
            if (strcmp(a->name, attr) == 0)
            {
                return CP_DUPE_ATTR;
            }
        }

        attributes.push_back(attribute(attr, t));
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
    PARSE_TOKEN(extract_uint32_t, space);
    start = end + 1;

    // Pull out the subspace id
    PARSE_TOKEN(extract_uint16_t, subspace);
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

        PARSE_TOKEN(extract_bool, repl);
        start = end + 1;

        PARSE_TOKEN(extract_bool, disk);
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
                case HYPERDATATYPE_FLOAT:
                    break;
                // Types which we cannot hash for searching
                case HYPERDATATYPE_LIST_STRING:
                case HYPERDATATYPE_LIST_INT64:
                case HYPERDATATYPE_LIST_FLOAT:
                case HYPERDATATYPE_SET_STRING:
                case HYPERDATATYPE_SET_INT64:
                case HYPERDATATYPE_SET_FLOAT:
                case HYPERDATATYPE_MAP_STRING_STRING:
                case HYPERDATATYPE_MAP_STRING_INT64:
                case HYPERDATATYPE_MAP_STRING_FLOAT:
                case HYPERDATATYPE_MAP_INT64_STRING:
                case HYPERDATATYPE_MAP_INT64_INT64:
                case HYPERDATATYPE_MAP_INT64_FLOAT:
                case HYPERDATATYPE_MAP_FLOAT_STRING:
                case HYPERDATATYPE_MAP_FLOAT_INT64:
                case HYPERDATATYPE_MAP_FLOAT_FLOAT:
                    return CP_ATTR_NOT_SEARCHABLE;
                case HYPERDATATYPE_GENERIC:
                case HYPERDATATYPE_LIST_GENERIC:
                case HYPERDATATYPE_SET_GENERIC:
                case HYPERDATATYPE_MAP_GENERIC:
                case HYPERDATATYPE_MAP_STRING_KEYONLY:
                case HYPERDATATYPE_MAP_INT64_KEYONLY:
                case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
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
    PARSE_TOKEN(extract_uint32_t, space);
    start = end + 1;

    // Pull out the subspace id
    PARSE_TOKEN(extract_uint16_t, subspace);
    start = end + 1;

    // Pull out the prefix
    PARSE_TOKEN(extract_uint8_t, prefix);
    start = end + 1;

    // Pull out the mask
    PARSE_TOKEN(extract_uint64_t, mask);
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
        PARSE_TOKEN(extract_uint64_t, id);
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
    PARSE_TOKEN(extract_uint16_t, xfer_id);
    start = end + 1;
    // Pull out the space id
    PARSE_TOKEN(extract_uint32_t, space);
    start = end + 1;
    // Pull out the subspace id
    PARSE_TOKEN(extract_uint16_t, subspace);
    start = end + 1;
    // Pull out the prefix
    PARSE_TOKEN(extract_uint8_t, prefix);
    start = end + 1;
    // Pull out the mask
    PARSE_TOKEN(extract_uint64_t, mask);
    start = end + 1;
    // Pull out the host's numeric id
    PARSE_TOKEN(extract_uint64_t, host_id);
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
    char* quiesce_state_id;

    // Skip "quiesce "
    start += 8;

    // Pull out the quiesce state id
    PARSE_TOKEN(extract_cstring, quiesce_state_id);
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
    else if (strcmp(start, "float") == 0)
    {
        *t = HYPERDATATYPE_FLOAT;
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
    else if (strcmp(start, "list(float)") == 0)
    {
        *t = HYPERDATATYPE_LIST_FLOAT;
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
    else if (strcmp(start, "set(float)") == 0)
    {
        *t = HYPERDATATYPE_SET_FLOAT;
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
    else if (strcmp(start, "map(float,string)") == 0)
    {
        *t = HYPERDATATYPE_MAP_FLOAT_STRING;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(float,int64)") == 0)
    {
        *t = HYPERDATATYPE_MAP_FLOAT_INT64;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(float,float)") == 0)
    {
        *t = HYPERDATATYPE_MAP_FLOAT_FLOAT;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(string,float)") == 0)
    {
        *t = HYPERDATATYPE_MAP_STRING_FLOAT;
        return CP_SUCCESS;
    }
    else if (strcmp(start, "map(int64,float)") == 0)
    {
        *t = HYPERDATATYPE_MAP_INT64_FLOAT;
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
hyperdex :: configuration_parser :: extract_cstring(char* start,
                                                    char*,
                                                    char** ptr)
{
    *ptr = start;
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
                case HYPERDATATYPE_FLOAT:
                    hfuncs.push_back(hyperspacehashing::RANGE);
                    break;
                case HYPERDATATYPE_GENERIC:
                case HYPERDATATYPE_LIST_GENERIC:
                case HYPERDATATYPE_LIST_STRING:
                case HYPERDATATYPE_LIST_INT64:
                case HYPERDATATYPE_LIST_FLOAT:
                case HYPERDATATYPE_SET_GENERIC:
                case HYPERDATATYPE_SET_STRING:
                case HYPERDATATYPE_SET_INT64:
                case HYPERDATATYPE_SET_FLOAT:
                case HYPERDATATYPE_MAP_GENERIC:
                case HYPERDATATYPE_MAP_STRING_KEYONLY:
                case HYPERDATATYPE_MAP_STRING_STRING:
                case HYPERDATATYPE_MAP_STRING_INT64:
                case HYPERDATATYPE_MAP_STRING_FLOAT:
                case HYPERDATATYPE_MAP_INT64_KEYONLY:
                case HYPERDATATYPE_MAP_INT64_STRING:
                case HYPERDATATYPE_MAP_INT64_INT64:
                case HYPERDATATYPE_MAP_INT64_FLOAT:
                case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
                case HYPERDATATYPE_MAP_FLOAT_STRING:
                case HYPERDATATYPE_MAP_FLOAT_INT64:
                case HYPERDATATYPE_MAP_FLOAT_FLOAT:
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
