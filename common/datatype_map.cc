// Copyright (c) 2013, Cornell University
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
#include <algorithm>

// e
#include <e/endian.h>
#include <e/safe_math.h>

// HyperDex
#include "common/datatype_map.h"

using hyperdex::datatype_map;

datatype_map :: datatype_map(datatype_info* k, datatype_info* v)
    : m_k(k)
    , m_v(v)
{
    assert(m_k->containable() && m_k->comparable());
    assert(m_v->containable() && m_v->comparable());
}

datatype_map :: ~datatype_map() throw ()
{
}

hyperdatatype
datatype_map :: datatype() const
{
    return CREATE_CONTAINER2(HYPERDATATYPE_MAP_GENERIC, m_k->datatype(), m_v->datatype());
}

bool
datatype_map :: validate(const e::slice& map) const
{
    const uint8_t* ptr = map.data();
    const uint8_t* end = map.data() + map.size();
    e::slice key;
    e::slice val;
    e::slice old;
    bool has_old = false;

    while (ptr < end)
    {
        if (!m_k->step(&ptr, end, &key))
        {
            return false;
        }

        if (!m_v->step(&ptr, end, &val))
        {
            return false;
        }

        if (has_old)
        {
            if (m_k->compare(old, key) >= 0)
            {
                return false;
            }
        }

        old = key;
        has_old = true;
    }

    return ptr == end;
}

bool
datatype_map :: check_args(const funcall& func) const
{
    // depending on the operation the arguments may differ
    // we must ensure that they match

    // set needs a whole map as an argument
    if(func.name == FUNC_SET)
    {
        return (func.arg1_datatype == datatype() ||
             func.arg1_datatype == HYPERDATATYPE_MAP_GENERIC)
             && validate(func.arg1);
    }
    // inserting a new element needs a key/value-pair
    else if(func.name == FUNC_MAP_ADD)
    {
        return m_v->validate(func.arg1) &&
            m_k->validate(func.arg2) &&
            func.arg1_datatype == m_v->datatype() &&
            func.arg2_datatype == m_k->datatype();
    }
    // Remove only needs a key
    else if(func.name == FUNC_MAP_REMOVE)
    {
        return m_k->validate(func.arg1) && func.arg1_datatype == m_k->datatype();
    }
    // Other operations embed their arguments in the second datatype
    else if(func.name == FUNC_STRING_APPEND ||
            func.name == FUNC_STRING_PREPEND ||
            func.name == FUNC_NUM_ADD ||
            func.name == FUNC_NUM_SUB ||
            func.name == FUNC_NUM_MUL ||
            func.name == FUNC_NUM_DIV ||
            func.name == FUNC_NUM_MOD ||
            func.name == FUNC_NUM_AND ||
            func.name == FUNC_NUM_OR  ||
            func.name == FUNC_NUM_XOR ||
            func.name == FUNC_NUM_MIN ||
            func.name == FUNC_NUM_MAX)
    {
        return m_k->validate(func.arg2)
             && func.arg2_datatype == m_k->datatype()
             && m_v->check_args(func);
    }
    else
    {
        return false;
    }
}

bool
datatype_map :: apply(const e::slice& old_value,
                      const funcall* funcs, size_t funcs_sz,
                      e::arena* new_memory,
                      e::slice* new_value) const
{
    // Initialize map with the compare operator of the key's datatype
    map_t map(m_k->compare_less());

    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice key;
    e::slice val;

    // Read previous data into the map
    while (ptr < end)
    {
        bool stepped;
        stepped = m_k->step(&ptr, end, &key);
        assert(stepped);
        stepped = m_v->step(&ptr, end, &val);
        assert(stepped);
        map.insert(std::make_pair(key, val));
    }

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
                // Discard current content and insert a new key-value-pair
                map.clear();
                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    bool stepped;
                    stepped = m_k->step(&ptr, end, &key);
                    assert(stepped);
                    stepped = m_v->step(&ptr, end, &val);
                    assert(stepped);
                    map.insert(std::make_pair(key, val));
                }

                break;
            case FUNC_MAP_ADD:
                map[funcs[i].arg2] = funcs[i].arg1;
                break;
            case FUNC_MAP_REMOVE:
                map.erase(funcs[i].arg1);
                break;
            case FUNC_STRING_APPEND:
            case FUNC_STRING_PREPEND:
            case FUNC_STRING_LTRIM:
            case FUNC_STRING_RTRIM:
            case FUNC_NUM_ADD:
            case FUNC_NUM_MIN:
            case FUNC_NUM_MAX:
            case FUNC_NUM_SUB:
            case FUNC_NUM_MUL:
            case FUNC_NUM_DIV:
            case FUNC_NUM_MOD:
            case FUNC_NUM_AND:
            case FUNC_NUM_OR:
            case FUNC_NUM_XOR:
                // This function is a composite of several subfunctions
                if (!apply_inner(&map, funcs + i, new_memory))
                {
                    return false;
                }

                break;
            case FUNC_FAIL:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_DOC_RENAME:
            case FUNC_DOC_UNSET:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            default:
                abort();
        }
    }

    size_t sz = 0;

    for (map_t::iterator i = map.begin(); i != map.end(); ++i)
    {
        sz += m_k->write_sz(i->first);
        sz += m_v->write_sz(i->second);
    }

    uint8_t* write_to = NULL;
    new_memory->allocate(sz, &write_to);
    *new_value = e::slice(write_to, sz);

    for (map_t::iterator i = map.begin(); i != map.end(); ++i)
    {
        write_to = m_k->write(i->first, write_to);
        write_to = m_v->write(i->second, write_to);
    }

    return true;
}

bool
datatype_map :: apply_inner(map_t* m,
                            const funcall* func,
                            e::arena* new_memory) const
{
    map_t::iterator it = m->find(func->arg2);
    e::slice old_value("", 0);

    if (it != m->end())
    {
        old_value = it->second;
    }

    e::slice new_value;

    if (!m_v->apply(old_value, func, 1, new_memory, &new_value))
    {
        return false;
    }

    (*m)[func->arg2] = new_value;
    return true;
}

bool
datatype_map :: indexable() const
{
    return m_k->indexable();
}

bool
datatype_map :: has_length() const
{
    return true;
}

uint64_t
datatype_map :: length(const e::slice& map) const
{
    const uint8_t* ptr = map.data();
    const uint8_t* end = map.data() + map.size();
    e::slice key;
    e::slice val;
    uint64_t count = 0;

    while (ptr < end)
    {
        bool stepped;
        stepped = m_k->step(&ptr, end, &key);
        assert(stepped);
        stepped = m_v->step(&ptr, end, &val);
        assert(stepped);
        ++count;
    }

    assert(ptr == end);
    return count;
}

bool
datatype_map :: has_contains() const
{
    return true;
}

hyperdatatype
datatype_map :: contains_datatype() const
{
    return m_k->datatype();
}

bool
datatype_map :: contains(const e::slice& map, const e::slice& needle) const
{
    const uint8_t* ptr = map.data();
    const uint8_t* end = map.data() + map.size();
    e::slice key;
    e::slice val;

    while (ptr < end)
    {
        bool stepped;
        stepped = m_k->step(&ptr, end, &key);
        assert(stepped);
        stepped = m_v->step(&ptr, end, &val);
        assert(stepped);

        if (key == needle)
        {
            return true;
        }
    }

    assert(ptr == end);
    return false;
}
