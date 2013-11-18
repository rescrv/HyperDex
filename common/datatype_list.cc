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
#include <list>

// e
#include <e/endian.h>
#include <e/safe_math.h>

// HyperDex
#include "common/datatype_list.h"

using hyperdex::datatype_list;

datatype_list :: datatype_list(datatype_info* elem)
    : m_elem(elem)
{
    assert(m_elem->containable());
}

datatype_list :: ~datatype_list() throw ()
{
}

hyperdatatype
datatype_list :: datatype()
{
    return CREATE_CONTAINER(HYPERDATATYPE_LIST_GENERIC, m_elem->datatype());
}

bool
datatype_list :: validate(const e::slice& list)
{
    const uint8_t* ptr = list.data();
    const uint8_t* end = list.data() + list.size();
    e::slice elem;

    while (ptr < end)
    {
        if (!m_elem->step(&ptr, end, &elem))
        {
            return false;
        }
    }

    return ptr == end;
}

bool
datatype_list :: check_args(const funcall& func)
{
    return ((func.arg1_datatype == datatype() ||
             func.arg1_datatype == HYPERDATATYPE_LIST_GENERIC) &&
            validate(func.arg1) && func.name == FUNC_SET) ||
           (func.arg1_datatype == m_elem->datatype() &&
            m_elem->validate(func.arg1) &&
            (func.name == FUNC_LIST_LPUSH ||
             func.name == FUNC_LIST_RPUSH));
}

uint8_t*
datatype_list :: apply(const e::slice& old_value,
                       const funcall* funcs, size_t funcs_sz,
                       uint8_t* writeto)
{
    std::list<e::slice> list;
    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice elem;

    while (ptr < end)
    {
        bool stepped = m_elem->step(&ptr, end, &elem);
        assert(stepped); // safe because of check_args
        list.push_back(elem);
    }

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
                list.clear();
                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    bool stepped = m_elem->step(&ptr, end, &elem);
                    assert(stepped); // safe because of check_args
                    list.push_back(elem);
                }

                break;
            case FUNC_LIST_LPUSH:
                list.push_front(funcs[i].arg1);
                break;
            case FUNC_LIST_RPUSH:
                list.push_back(funcs[i].arg1);
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
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            default:
                abort();
        }
    }

    for (std::list<e::slice>::iterator i = list.begin(); i != list.end(); ++i)
    {
        writeto = m_elem->write(writeto, *i);
    }

    return writeto;
}

bool
datatype_list :: indexable()
{
    return m_elem->indexable();
}

bool
datatype_list :: has_length()
{
    return true;
}

uint64_t
datatype_list :: length(const e::slice& list)
{
    const uint8_t* ptr = list.data();
    const uint8_t* end = list.data() + list.size();
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
datatype_list :: has_contains()
{
    return true;
}

hyperdatatype
datatype_list :: contains_datatype()
{
    return m_elem->datatype();
}

bool
datatype_list :: contains(const e::slice& list, const e::slice& needle)
{
    const uint8_t* ptr = list.data();
    const uint8_t* end = list.data() + list.size();
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
