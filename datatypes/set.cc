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
#include <set>

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/compare.h"
#include "datatypes/set.h"
#include "datatypes/sort.h"
#include "datatypes/step.h"
#include "datatypes/write.h"

bool
validate_set(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
             int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
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
            int cmp = compare_elem(old, elem);

            if (cmp >= 0)
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

uint8_t*
apply_set_add_remove(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                     bool (*validate_elem)(const e::slice& elem),
                     int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
                     uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
                     const e::slice& old_value,
                     microop* ops, size_t num_ops,
                     uint8_t* writeto, microerror* error)
{
    assert(num_ops > 0);

    if (!validate_elem(ops->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    // Verify sorted order of the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        if (!validate_elem(ops[i + 1].arg1))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (compare_elem(ops[i].arg1, ops[i + 1].arg1) >= 0)
        {
            *error = MICROERROR;
            return NULL;
        }
    }

    sort_microops_by_arg1(ops, ops + num_ops, compare_elem);
    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        const uint8_t* next = ptr;
        e::slice elem;

        if (!step_elem(&next, end, &elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        int cmp = compare_elem(elem, ops[i].arg1);

        if (cmp < 0)
        {
            writeto = write_elem(writeto, elem);
            ptr = next;
        }
        else if (cmp > 0)
        {
            switch (ops[i].action)
            {
                case OP_SET_ADD:
                    writeto = write_elem(writeto, ops[i].arg1);
                    ++i;
                    break;
                case OP_SET_REMOVE:
                    // Nothing to remove
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
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
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_MAP_ADD:
                case OP_MAP_REMOVE:
                case OP_SET:
                case OP_FAIL:
                default:
                    *error = MICROERROR;
                    return NULL;
            }
        }
        else
        {
            switch (ops[i].action)
            {
                case OP_SET_ADD:
                    writeto = write_elem(writeto, ops[i].arg1);
                    ptr = next;
                    ++i;
                    break;
                case OP_SET_REMOVE:
                    ptr = next;
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
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
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_MAP_ADD:
                case OP_MAP_REMOVE:
                case OP_SET:
                case OP_FAIL:
                default:
                    *error = MICROERROR;
                    return NULL;
            }
        }
    }

    while (ptr < end)
    {
        e::slice elem;

        if (!step_elem(&ptr, end, &elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_elem(writeto, elem);
    }

    while (i < num_ops)
    {
        switch (ops[i].action)
        {
            case OP_SET_ADD:
                writeto = write_elem(writeto, ops[i].arg1);
                ++i;
                break;
            case OP_SET_REMOVE:
                ++i;
                break;
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
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
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            case OP_SET:
            case OP_FAIL:
            default:
                *error = MICROERROR;
                return NULL;
        }
    }

    return writeto;
}

uint8_t*
apply_set_set(bool (*validate_set)(const e::slice& data),
              const microop* op,
              uint8_t* writeto, microerror* error)
{
    assert(op->action == OP_SET);

    if (!validate_set(op->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    memmove(writeto, op->arg1.data(), op->arg1.size());
    return writeto + op->arg1.size();
}

uint8_t*
apply_set_intersect(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                    int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
                    uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
                    const e::slice& old_value,
                    const microop* op,
                    uint8_t* writeto, microerror* error)
{
    // There's an efficiency tradeoff in this function.  We could validate that
    // the user-provided set is sorted, and then run a merge algorithm, or we
    // could validate that it's sorted on the fly.  It's easier to pre-validate
    // that the set is sorted, and the efficiency trade-off is on the order of
    // constants.
    assert(op->action == OP_SET_INTERSECT);

    if (!validate_set(step_elem, compare_elem, op->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    const uint8_t* set_ptr = old_value.data();
    const uint8_t* const set_end = old_value.data() + old_value.size();
    const uint8_t* new_ptr = op->arg1.data();
    const uint8_t* const new_end = op->arg1.data() + op->arg1.size();

    // We only need to consider this case.
    while (set_ptr < set_end && new_ptr < new_end)
    {
        const uint8_t* next_set_ptr = set_ptr;
        const uint8_t* next_new_ptr = new_ptr;
        e::slice set_elem;
        e::slice new_elem;

        if (!step_elem(&next_set_ptr, set_end, &set_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_elem(&next_new_ptr, new_end, &new_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        int cmp = compare_elem(set_elem, new_elem);

        if (cmp < 0)
        {
            set_ptr = next_set_ptr;
        }
        else if (cmp > 0)
        {
            new_ptr = next_new_ptr;
        }
        else
        {
            writeto = write_elem(writeto, set_elem);
            set_ptr = next_set_ptr;
            new_ptr = next_new_ptr;
        }
    }

    return writeto;
}

uint8_t*
apply_set_union(bool (*step_elem)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                int (*compare_elem)(const e::slice& lhs, const e::slice& rhs),
                uint8_t* (*write_elem)(uint8_t* writeto, const e::slice& elem),
                const e::slice& old_value,
                const microop* op,
                uint8_t* writeto, microerror* error)
{
    assert(op->action == OP_SET_UNION);

    if (!validate_set(step_elem, compare_elem, op->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    const uint8_t* set_ptr = old_value.data();
    const uint8_t* const set_end = old_value.data() + old_value.size();
    const uint8_t* new_ptr = op->arg1.data();
    const uint8_t* const new_end = op->arg1.data() + op->arg1.size();

    while (set_ptr < set_end && new_ptr < new_end)
    {
        const uint8_t* next_set_ptr = set_ptr;
        const uint8_t* next_new_ptr = new_ptr;
        e::slice set_elem;
        e::slice new_elem;

        if (!step_elem(&next_set_ptr, set_end, &set_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_elem(&next_new_ptr, new_end, &new_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        int cmp = compare_elem(set_elem, new_elem);

        if (cmp < 0)
        {
            writeto = write_elem(writeto, set_elem);
            set_ptr = next_set_ptr;
        }
        else if (cmp > 0)
        {
            writeto = write_elem(writeto, new_elem);
            new_ptr = next_new_ptr;
        }
        else
        {
            writeto = write_elem(writeto, set_elem);
            set_ptr = next_set_ptr;
            new_ptr = next_new_ptr;
        }
    }

    while (set_ptr < set_end)
    {
        e::slice set_elem;

        if (!step_elem(&set_ptr, set_end, &set_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_elem(writeto, set_elem);
    }

    while (new_ptr < new_end)
    {
        e::slice new_elem;

        if (!step_elem(&new_ptr, new_end, &new_elem))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_elem(writeto, new_elem);
    }

    return writeto;
}

#define APPLY_SET(TYPE) \
    uint8_t* \
    apply_set_ ## TYPE(const e::slice& old_value, \
                       microop* ops, size_t num_ops, \
                       uint8_t* writeto, microerror* error) \
    { \
        assert(num_ops > 0); \
     \
        if (ops[0].action == OP_SET_ADD || \
            ops[0].action == OP_SET_REMOVE) \
        { \
            return apply_set_add_remove(step_ ## TYPE, validate_as_ ## TYPE, compare_ ## TYPE, write_ ## TYPE, \
                                        old_value, ops, num_ops, writeto, error); \
        } \
        else if (ops[0].action == OP_SET && num_ops == 1) \
        { \
            return apply_set_set(validate_as_set_ ## TYPE, ops, writeto, error); \
        } \
        else if (ops[0].action == OP_SET_INTERSECT && num_ops == 1) \
        { \
            return apply_set_intersect(step_ ## TYPE, compare_ ## TYPE, write_ ## TYPE, \
                                       old_value, ops, writeto, error); \
        } \
        else if (ops[0].action == OP_SET_UNION && num_ops == 1) \
        { \
            return apply_set_union(step_ ## TYPE, compare_ ## TYPE, write_ ## TYPE, \
                                   old_value, ops, writeto, error); \
        } \
        else \
        { \
            *error = MICROERROR; \
            return NULL; \
        } \
    }

APPLY_SET(string)
APPLY_SET(int64)
APPLY_SET(float)
