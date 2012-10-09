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

// STL
#include <algorithm>
#include <set>

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/compare.h"
#include "datatypes/set.h"
#include "datatypes/step.h"
#include "datatypes/write.h"

using hyperdex::funcall;

bool
validate_set(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
             bool (*compare_elem_less)(const e::slice& lhs, const e::slice& rhs),
             const e::slice& set)
{
    const uint8_t* ptr = set.data();
    const uint8_t* end = set.data() + set.size();
    e::slice elem;
    e::slice old;
    bool has_old = false;

    while (ptr < end)
    {
        if (!step_elem(&ptr, end, &elem))
        {
            return false;
        }

        if (has_old)
        {
            if (!compare_elem_less(old, elem))
            {
                return false;
            }
        }

        old = elem;
        has_old = true;
    }

    return ptr == end;
}

#define VALIDATE_SET(TYPE) \
    bool \
    validate_as_set_ ## TYPE(const e::slice& value) \
    { \
        return validate_set(step_ ## TYPE, compare_ ## TYPE, value); \
    }

VALIDATE_SET(string)
VALIDATE_SET(int64)
VALIDATE_SET(float)

static uint8_t*
apply_set(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
          bool (*validate_elem)(const e::slice& elem),
          bool (*compare_elem_less)(const e::slice& lhs, const e::slice& rhs),
          uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
          hyperdatatype container, hyperdatatype element,
          const e::slice& old_value,
          const funcall* funcs, size_t num_funcs,
          uint8_t* writeto, microerror* error)
{
    std::set<e::slice> set;
    std::set<e::slice> tmp;
    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice elem;

    while (ptr < end)
    {
        if (!step_elem(&ptr, end, &elem))
        {
            *error = MICROERR_MALFORMED;
            return NULL;
        }

        set.insert(elem);
    }

    for (size_t i = 0; i < num_funcs; ++i)
    {
        switch (funcs[i].name)
        {
            case hyperdex::FUNC_SET:
                set.clear();
            case hyperdex::FUNC_SET_UNION:
                if (funcs[i].arg1_datatype == HYPERDATATYPE_SET_GENERIC &&
                    funcs[i].arg1.size() == 0)
                {
                    continue;
                }
                else if (funcs[i].arg1_datatype == HYPERDATATYPE_SET_GENERIC)
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                if (container != funcs[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    if (!step_elem(&ptr, end, &elem))
                    {
                        *error = MICROERR_MALFORMED;
                        return NULL;
                    }

                    set.insert(elem);
                }

                break;
            case hyperdex::FUNC_SET_ADD:
                if (element != funcs[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                if (!validate_elem(funcs[i].arg1))
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                set.insert(funcs[i].arg1);
                break;
            case hyperdex::FUNC_SET_REMOVE:
                if (element != funcs[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                if (!validate_elem(funcs[i].arg1))
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                set.erase(funcs[i].arg1);
                break;
            case hyperdex::FUNC_SET_INTERSECT:
                if (funcs[i].arg1_datatype == HYPERDATATYPE_SET_GENERIC &&
                    funcs[i].arg1.size() == 0)
                {
                    set.clear();
                    continue;
                }
                else if (funcs[i].arg1_datatype == HYPERDATATYPE_SET_GENERIC)
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                if (container != funcs[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                tmp.clear();
                ptr = funcs[i].arg1.data();
                end = funcs[i].arg1.data() + funcs[i].arg1.size();

                while (ptr < end)
                {
                    if (!step_elem(&ptr, end, &elem))
                    {
                        *error = MICROERR_MALFORMED;
                        return NULL;
                    }

                    if (set.find(elem) != set.end())
                    {
                        tmp.insert(elem);
                    }
                }

                set.swap(tmp);
                break;
            case hyperdex::FUNC_FAIL:
            case hyperdex::FUNC_STRING_APPEND:
            case hyperdex::FUNC_STRING_PREPEND:
            case hyperdex::FUNC_NUM_ADD:
            case hyperdex::FUNC_NUM_SUB:
            case hyperdex::FUNC_NUM_MUL:
            case hyperdex::FUNC_NUM_DIV:
            case hyperdex::FUNC_NUM_MOD:
            case hyperdex::FUNC_NUM_AND:
            case hyperdex::FUNC_NUM_OR:
            case hyperdex::FUNC_NUM_XOR:
            case hyperdex::FUNC_LIST_LPUSH:
            case hyperdex::FUNC_LIST_RPUSH:
            case hyperdex::FUNC_MAP_ADD:
            case hyperdex::FUNC_MAP_REMOVE:
            default:
                *error = MICROERR_WRONGACTION;
                return NULL;
        }
    }

    std::vector<e::slice> v(set.begin(), set.end());
    std::sort(v.begin(), v.end(), compare_elem_less);

    for (size_t i = 0; i < v.size(); ++i)
    {
        writeto = write_elem(writeto, v[i]);
    }

    return writeto;
}

#define APPLY_SET(TYPE, TYPECAPS) \
    uint8_t* \
    apply_set_ ## TYPE(const e::slice& old_value, \
                       const funcall* funcs, size_t num_funcs, \
                       uint8_t* writeto, microerror* error) \
    { \
        return apply_set(step_ ## TYPE, validate_as_ ## TYPE, compare_ ## TYPE, write_ ## TYPE, \
                         HYPERDATATYPE_SET_ ## TYPECAPS, HYPERDATATYPE_ ## TYPECAPS, \
                         old_value, funcs, num_funcs, writeto, error); \
    }

APPLY_SET(string, STRING)
APPLY_SET(int64, INT64)
APPLY_SET(float, FLOAT)
