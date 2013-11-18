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

// C
#include <cstdlib>

// STL
#include <algorithm>
#include <set>

// e
#include <e/endian.h>
#include <e/safe_math.h>

// HyperDex
#include "common/datatype_set.h"

using hyperdex::datatype_set;

datatype_set :: datatype_set(datatype_info* elem)
    : m_elem(elem)
{
    assert(m_elem->containable() && m_elem->comparable());
}

datatype_set :: ~datatype_set() throw ()
{
}

hyperdatatype
datatype_set :: datatype()
{
    return CREATE_CONTAINER(HYPERDATATYPE_SET_GENERIC, m_elem->datatype());
}

bool
datatype_set :: validate(const e::slice& set)
{
    const uint8_t* ptr = set.data();
    const uint8_t* end = set.data() + set.size();
    e::slice elem;
    e::slice old;
    bool has_old = false;

    while (ptr < end)
    {
        if (!m_elem->step(&ptr, end, &elem))
        {
            return false;
        }

        if (has_old)
        {
            if (m_elem->compare(old, elem) >= 0)
            {
                return false;
            }
        }

        old = elem;
        has_old = true;
    }

    return ptr == end;
}

bool
datatype_set :: check_args(const funcall& func)
{
    return ((func.arg1_datatype == datatype() ||
             func.arg1_datatype == HYPERDATATYPE_SET_GENERIC) &&
            validate(func.arg1) &&
            (func.name == FUNC_SET ||
             func.name == FUNC_SET_UNION ||
             func.name == FUNC_SET_INTERSECT)) ||
           (func.arg1_datatype == m_elem->datatype() &&
            m_elem->validate(func.arg1) &&
            (func.name == FUNC_SET_ADD ||
             func.name == FUNC_SET_REMOVE));
}

uint8_t*
datatype_set :: apply(const e::slice& old_value,
                      const funcall* funcs, size_t funcs_sz,
                      uint8_t* writeto)
{
    typedef std::set<e::slice, datatype_info::compares_less> set_t;
    set_t set(m_elem->compare_less());
    set_t tmp(m_elem->compare_less());
    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice elem;

    while (ptr < end)
    {
        bool stepped = m_elem->step(&ptr, end, &elem);
        assert(stepped); // safe because of check_args
        set.insert(elem);
    }

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
                set.clear();
                // intentional fall-through
            case FUNC_SET_UNION:
                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    bool stepped = m_elem->step(&ptr, end, &elem);
                    assert(stepped); // safe because of check_args
                    set.insert(elem);
                }

                break;
            case FUNC_SET_ADD:
                set.insert(funcs[i].arg1);
                break;
            case FUNC_SET_REMOVE:
                set.erase(funcs[i].arg1);
                break;
            case FUNC_SET_INTERSECT:
                tmp.clear();
                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    bool stepped = m_elem->step(&ptr, end, &elem);
                    assert(stepped); // safe because of check_args

                    if (set.find(elem) != set.end())
                    {
                        tmp.insert(elem);
                    }
                }

                set.swap(tmp);
                break;
            case FUNC_FAIL:
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
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            default:
                abort();
        }
    }

    for (set_t::iterator i = set.begin(); i != set.end(); ++i)
    {
        writeto = m_elem->write(writeto, *i);
    }

    return writeto;
}

bool
datatype_set :: indexable()
{
    return m_elem->indexable();
}

bool
datatype_set :: has_length()
{
    return true;
}

uint64_t
datatype_set :: length(const e::slice& set)
{
    const uint8_t* ptr = set.data();
    const uint8_t* end = set.data() + set.size();
    e::slice elem;
    uint64_t count = 0;

    while (ptr < end)
    {
        bool stepped = m_elem->step(&ptr, end, &elem);
        assert(stepped);
        ++count;
    }

    assert(ptr == end);
    return count;
}

bool
datatype_set :: has_contains()
{
    return true;
}

hyperdatatype
datatype_set :: contains_datatype()
{
    return m_elem->datatype();
}

bool
datatype_set :: contains(const e::slice& set, const e::slice& needle)
{
    const uint8_t* ptr = set.data();
    const uint8_t* end = set.data() + set.size();
    e::slice elem;

    while (ptr < end)
    {
        bool stepped = m_elem->step(&ptr, end, &elem);
        assert(stepped);

        if (elem == needle)
        {
            return true;
        }
    }

    assert(ptr == end);
    return false;
}
