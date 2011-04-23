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

// C
#include <cassert>
#include <cstdlib>
#include <cstring>

// POSIX
#include <arpa/inet.h>

// STL
#include <stdexcept>

// HyperDex
#include <hyperdex/entity.h>

hyperdex :: entity :: entity()
    : type(UNDEF)
    , table(0)
    , subspace(0)
    , zone(0)
    , number(0)
{
}

hyperdex :: entity :: entity(entity_type _type,
                             uint32_t _table,
                             uint16_t _subspace,
                             uint16_t _zone,
                             uint8_t _number)
    : type(_type)
    , table(_table)
    , subspace(_subspace)
    , zone(_zone)
    , number(_number)
{
}

static inline void
unpack_32_16_16(const char* buf, uint32_t* a, uint16_t* b, uint16_t* c)
{
    *a = htonl(*a);
    *b = htons(*b);
    *c = htons(*c);
    memmove(a, buf, sizeof(uint32_t));
    memmove(b, buf + sizeof(uint32_t), sizeof(uint16_t));
    memmove(c, buf + sizeof(uint32_t) + sizeof(uint16_t), sizeof(uint16_t));
}

hyperdex :: entity :: entity(const char* buf)
    : type(UNDEF)
    , table(0)
    , subspace(0)
    , zone(0)
    , number(0)
{
    size_t offset = 0;
    uint8_t t;

    memmove(&t, buf, sizeof(uint8_t));
    offset += sizeof(uint8_t);
    memmove(&table, buf + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memmove(&subspace, buf + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memmove(&zone, buf + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memmove(&number, buf + offset, sizeof(uint8_t));

    type = static_cast<entity_type>(t);

    switch (type)
    {
        case UNDEF:
        case MASTER:
        case CLIENT_PROXY:
        case CHAIN_HEAD:
        case CHAIN_TAIL:
        case CHAIN_REPLICA:
            break;
        default:
            throw std::invalid_argument("Invalid entity type.");
    }

    table = ntohl(table);
    subspace = ntohs(subspace);
    zone = ntohs(zone);
}

hyperdex :: entity :: ~entity()
{
}

void
hyperdex :: entity :: serialize(char* buf) const
{
    uint8_t t = type & 0xff;
    uint32_t a = htonl(table);
    uint16_t b = htons(subspace);
    uint16_t c = htons(zone);
    uint8_t d = number;
    size_t offset = 0;

    memmove(buf, &t, sizeof(uint8_t));
    offset += sizeof(uint8_t);
    memmove(buf + offset, &a, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memmove(buf + offset, &b, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memmove(buf + offset, &c, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memmove(buf + offset, &d, sizeof(uint8_t));
}

int
hyperdex :: entity :: compare(const entity& rhs) const
{
    const entity& lhs(*this);

    if (lhs.type < rhs.type)
    {
        return -1;
    }
    else if (lhs.type > rhs.type)
    {
        return 1;
    }

    if (lhs.table < rhs.table)
    {
        return -1;
    }
    else if (lhs.table > rhs.table)
    {
        return 1;
    }

    if (lhs.subspace < rhs.subspace)
    {
        return -1;
    }
    else if (lhs.subspace > rhs.subspace)
    {
        return 1;
    }

    if (lhs.zone < rhs.zone)
    {
        return -1;
    }
    else if (lhs.zone > rhs.zone)
    {
        return 1;
    }

    return lhs.number - rhs.number;
}
