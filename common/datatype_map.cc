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
datatype_map :: datatype()
{
    return CREATE_CONTAINER2(HYPERDATATYPE_MAP_GENERIC, m_k->datatype(), m_v->datatype());
}

bool
datatype_map :: validate(const e::slice& map)
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
datatype_map :: check_args(const funcall& func)
{
    return ((func.arg1_datatype == datatype() ||
             func.arg1_datatype == HYPERDATATYPE_MAP_GENERIC) &&
            validate(func.arg1) && func.name == FUNC_SET) ||
           (func.arg1_datatype == m_v->datatype() &&
            func.arg2_datatype == m_k->datatype() &&
            m_v->validate(func.arg1) &&
            m_k->validate(func.arg2) &&
            func.name == FUNC_MAP_ADD) ||
           (func.arg2_datatype == m_k->datatype() &&
            m_k->validate(func.arg2) &&
            func.name == FUNC_MAP_REMOVE) ||
           (func.arg2_datatype == m_k->datatype() &&
            m_k->validate(func.arg2) &&
            (func.name == FUNC_STRING_APPEND ||
             func.name == FUNC_STRING_PREPEND ||
             func.name == FUNC_NUM_ADD ||
             func.name == FUNC_NUM_SUB ||
             func.name == FUNC_NUM_MUL ||
             func.name == FUNC_NUM_DIV ||
             func.name == FUNC_NUM_MOD ||
             func.name == FUNC_NUM_AND ||
             func.name == FUNC_NUM_OR ||
             func.name == FUNC_NUM_XOR) &&
            m_v->check_args(func));
}

uint8_t*
datatype_map :: apply(const e::slice& old_value,
                      const funcall* funcs, size_t funcs_sz,
                      uint8_t* writeto)
{
    map_t map(m_k->compare_less());
    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice key;
    e::slice val;

    while (ptr < end)
    {
        bool stepped;
        stepped = m_k->step(&ptr, end, &key);
        assert(stepped);
        stepped = m_v->step(&ptr, end, &val);
        assert(stepped);
        map.insert(std::make_pair(key, val));
    }

    e::array_ptr<e::array_ptr<uint8_t> > scratch;
    scratch = new e::array_ptr<uint8_t>[funcs_sz];

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
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
                map.erase(funcs[i].arg2);
                break;
            case FUNC_STRING_APPEND:
            case FUNC_STRING_PREPEND:
            case FUNC_NUM_ADD:
            case FUNC_NUM_SUB:
            case FUNC_NUM_MUL:
            case FUNC_NUM_DIV:
            case FUNC_NUM_MOD:
            case FUNC_NUM_AND:
            case FUNC_NUM_OR:
            case FUNC_NUM_XOR:
                if (!apply_inner(&map, &scratch[i], funcs + i))
                {
                    return NULL;
                }

                break;
            case FUNC_FAIL:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            default:
                abort();
        }
    }

    for (map_t::iterator i = map.begin(); i != map.end(); ++i)
    {
        writeto = m_k->write(writeto, i->first);
        writeto = m_v->write(writeto, i->second);
    }

    return writeto;
}

bool
datatype_map :: apply_inner(map_t* m,
                            e::array_ptr<uint8_t>* scratch,
                            const funcall* func)
{
    map_t::iterator it = m->find(func->arg2);
    e::slice old_value("", 0);

    if (it != m->end())
    {
        old_value = it->second;
    }

    *scratch = new uint8_t[old_value.size() + sizeof(uint32_t) + func->arg1.size()];
    uint8_t* writeto = m_v->apply(old_value, func, 1, scratch->get());

    if (!writeto)
    {
        return NULL;
    }

    //writeto = m_v->write(scratch->get(), e::slice(scratch->get(), writeto - scratch->get()));
    (*m)[func->arg2] = e::slice(scratch->get(), writeto - scratch->get());
    return writeto;
}

bool
datatype_map :: indexable()
{
    return m_k->indexable();
}

bool
datatype_map :: has_length()
{
    return true;
}

uint64_t
datatype_map :: length(const e::slice& map)
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
datatype_map :: has_contains()
{
    return true;
}

hyperdatatype
datatype_map :: contains_datatype()
{
    return m_k->datatype();
}

bool
datatype_map :: contains(const e::slice& map, const e::slice& needle)
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
