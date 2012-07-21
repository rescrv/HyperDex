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
#include <list>

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/list.h"
#include "datatypes/step.h"
#include "datatypes/write.h"

static bool
validate_list(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
              const e::slice& list)
{
    const uint8_t* ptr = list.data();
    const uint8_t* end = list.data() + list.size();
    e::slice elem;

    while (ptr < end)
    {
        if (!step_elem(&ptr, end, &elem))
        {
            return false;
        }
    }

    return ptr == end;
}

#define VALIDATE_LIST(TYPE) \
    bool \
    validate_as_list_ ## TYPE (const e::slice& value) \
    { \
        return validate_list(step_ ## TYPE, value); \
    } \

VALIDATE_LIST(string)
VALIDATE_LIST(int64)
VALIDATE_LIST(float)

static uint8_t*
apply_list(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
           bool (*validate_elem)(const e::slice& elem),
           uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
           hyperdatatype container, hyperdatatype element,
           const e::slice& old_value,
           const microop* ops, size_t num_ops,
           uint8_t* writeto, microerror* error)
{
    std::list<e::slice> list;
    const uint8_t* ptr = old_value.data();
    const uint8_t* end = old_value.data() + old_value.size();
    e::slice elem;

    while (ptr < end)
    {
        if (!step_elem(&ptr, end, &elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        list.push_back(elem);
    }

    for (size_t i = 0; i < num_ops; ++i)
    {
        switch (ops[i].action)
        {
            case OP_SET:
                if (ops[i].arg1_datatype == HYPERDATATYPE_LIST_GENERIC &&
                    ops[i].arg1.size() == 0)
                {
                    list.clear();
                    continue;
                }
                else if (ops[i].arg1_datatype == HYPERDATATYPE_LIST_GENERIC)
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                if (container != ops[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                list.clear();
                ptr = ops[i].arg1.data();
                end = ops[i].arg1.data() + ops[i].arg1.size();

                while (ptr < end)
                {
                    if (!step_elem(&ptr, end, &elem))
                    {
                        *error = MICROERR_MALFORMED;
                        return NULL;
                    }

                    list.push_back(elem);
                }

                break;
            case OP_LIST_LPUSH:
                if (element != ops[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                if (!validate_elem(ops[i].arg1))
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                list.push_front(ops[i].arg1);
                break;
            case OP_LIST_RPUSH:
                if (element != ops[i].arg1_datatype)
                {
                    *error = MICROERR_WRONGTYPE;
                    return NULL;
                }

                if (!validate_elem(ops[i].arg1))
                {
                    *error = MICROERR_MALFORMED;
                    return NULL;
                }

                list.push_back(ops[i].arg1);
                break;
            case OP_FAIL:
            case OP_STRING_APPEND:
            case OP_STRING_PREPEND:
            case OP_NUM_ADD:
            case OP_NUM_SUB:
            case OP_NUM_MUL:
            case OP_NUM_DIV:
            case OP_NUM_MOD:
            case OP_NUM_AND:
            case OP_NUM_OR:
            case OP_NUM_XOR:
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            default:
                *error = MICROERR_WRONGACTION;
                return NULL;
        }
    }

    for (std::list<e::slice>::iterator i = list.begin(); i != list.end(); ++i)
    {
        writeto = write_elem(writeto, *i);
    }

    return writeto;
}

#define APPLY_LIST(TYPE, TYPECAPS) \
    uint8_t* \
    apply_list_ ## TYPE(const e::slice& old_value, \
                        const microop* ops, size_t num_ops, \
                        uint8_t* writeto, microerror* error) \
    { \
        return apply_list(step_ ## TYPE, validate_as_ ## TYPE, write_ ## TYPE, \
                          HYPERDATATYPE_LIST_ ## TYPECAPS, \
                          HYPERDATATYPE_ ## TYPECAPS, \
                          old_value, ops, num_ops, writeto, error); \
    }

APPLY_LIST(string, STRING)
APPLY_LIST(int64, INT64)
APPLY_LIST(float, FLOAT)
