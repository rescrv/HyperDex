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

uint8_t*
apply_list(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
           bool (*validate_elem)(const e::slice& elem),
           uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
           const e::slice& old_value,
           const microop* ops, size_t num_ops,
           uint8_t* writeto, microerror* error)
{
    if (num_ops == 1 && ops->action == OP_SET)
    {
        if (!validate_list(step_elem, ops->arg1))
        {
            *error = MICROERROR;
            return NULL;
        }

        memmove(writeto, ops->arg1.data(), ops->arg1.size());
        return writeto + ops->arg1.size();
    }

    // Apply the lpush ops
    for (ssize_t i = num_ops - 1; i >= 0; --i)
    {
        if (ops[i].action != OP_LIST_LPUSH)
        {
            continue;
        }

        if (!validate_elem(ops[i].arg1))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_elem(writeto, ops[i].arg1);
    }

    // Copy the middle of the list
    memmove(writeto, old_value.data(), old_value.size());
    writeto += old_value.size();

    // Apply the rpush ops
    for (size_t i = 0; i < num_ops; ++i)
    {
        if (ops[i].action != OP_LIST_RPUSH)
        {
            if (ops[i].action != OP_LIST_LPUSH)
            {
                *error = MICROERROR;
                return NULL;
            }

            continue;
        }

        if (!validate_elem(ops[i].arg1))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_elem(writeto, ops[i].arg1);
    }

    return writeto;
}

#define APPLY_LIST(TYPE) \
    uint8_t* \
    apply_list_ ## TYPE(const e::slice& old_value, \
                        const microop* ops, size_t num_ops, \
                        uint8_t* writeto, microerror* error) \
    { \
        return apply_list(step_ ## TYPE, validate_as_ ## TYPE, write_ ## TYPE, \
                          old_value, ops, num_ops, writeto, error); \
    }

APPLY_LIST(string)
APPLY_LIST(int64)
APPLY_LIST(float)
