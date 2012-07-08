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

// e
#include <e/endian.h>

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/compare.h"
#include "datatypes/set.h"
#include "datatypes/sort.h"
#include "datatypes/step.h"
#include "datatypes/write.h"

static bool
validate_map(bool (*step_key)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
             bool (*step_val)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
             int (*compare_key)(const e::slice& lhs, const e::slice& rhs),
             const e::slice& map)
{
    const uint8_t* ptr = map.data();
    const uint8_t* end = map.data() + map.size();
    e::slice key;
    e::slice val;
    e::slice old;
    bool has_old = false;

    while (ptr < end)
    {
        if (!step_key(&ptr, end, &key))
        {
            return false;
        }

        if (!step_val(&ptr, end, &val))
        {
            return false;
        }

        if (has_old)
        {
            int cmp = compare_key(old, key);

            if (cmp >= 0)
            {
                return false;
            }
        }

        old = key;
        has_old = true;
    }

    return ptr == end;
}

#define VALIDATE_MAP(KEY_T, VAL_T) \
    bool \
    validate_as_map_ ## KEY_T ## _ ## VAL_T(const e::slice& value) \
    { \
        return validate_map(step_ ## KEY_T, step_ ## VAL_T, compare_ ## KEY_T, value); \
    }

VALIDATE_MAP(string, string)
VALIDATE_MAP(string, int64)
VALIDATE_MAP(string, float)
VALIDATE_MAP(int64, string)
VALIDATE_MAP(int64, int64)
VALIDATE_MAP(int64, float)
VALIDATE_MAP(float, string)
VALIDATE_MAP(float, int64)
VALIDATE_MAP(float, float)

uint8_t*
apply_map_add_remove(bool (*step_key)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                     bool (*step_val)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                     bool (*validate_key)(const e::slice& elem),
                     bool (*validate_val)(const e::slice& elem),
                     int (*compare_key)(const e::slice& lhs, const e::slice& rhs),
                     uint8_t* (*write_key)(uint8_t* writeto, const e::slice& elem),
                     uint8_t* (*write_val)(uint8_t* writeto, const e::slice& elem),
                     const e::slice& old_value,
                     microop* ops, size_t num_ops,
                     uint8_t* writeto, microerror* error)
{
    assert(num_ops > 0);

    if ((ops->action != OP_MAP_REMOVE && !validate_val(ops->arg1)) ||
        !validate_key(ops->arg2))
    {
        *error = MICROERROR;
        return NULL;
    }

    // Validate and sort the the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        if ((ops->action != OP_MAP_REMOVE && !validate_val(ops[i + 1].arg1)) ||
            !validate_key(ops[i + 1].arg2))
        {
            *error = MICROERROR;
            return NULL;
        }
    }

    sort_microops_by_arg2(ops, ops + num_ops, compare_key);
    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        const uint8_t* next = ptr;
        e::slice key;
        e::slice val;

        if (!step_key(&next, end, &key))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_val(&next, end, &val))
        {
            *error = MICROERROR;
            return NULL;
        }

        int cmp = compare_key(key, ops[i].arg2);

        if (cmp < 0)
        {
            writeto = write_key(writeto, key);
            writeto = write_val(writeto, val);
            ptr = next;
        }
        else if (cmp > 0)
        {
            switch (ops[i].action)
            {
                case OP_MAP_ADD:
                    writeto = write_key(writeto, ops[i].arg2);
                    writeto = write_val(writeto, ops[i].arg1);
                    ++i;
                    break;
                case OP_MAP_REMOVE:
                    // Nothing to remove
                    ++i;
                    break;
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
                case OP_SET_ADD:
                case OP_SET_REMOVE:
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
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
                case OP_MAP_ADD:
                    writeto = write_key(writeto, ops[i].arg2);
                    writeto = write_val(writeto, ops[i].arg1);
                    ptr = next;
                    ++i;
                    break;
                case OP_MAP_REMOVE:
                    ptr = next;
                    ++i;
                    break;
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
                case OP_SET_ADD:
                case OP_SET_REMOVE:
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
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
        e::slice key;
        e::slice val;

        if (!step_key(&ptr, end, &key))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_val(&ptr, end, &val))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_key(writeto, key);
        writeto = write_val(writeto, val);
    }

    while (i < num_ops)
    {
        switch (ops[i].action)
        {
            case OP_MAP_ADD:
                writeto = write_key(writeto, ops[i].arg2);
                writeto = write_val(writeto, ops[i].arg1);
                ++i;
                break;
            case OP_MAP_REMOVE:
                ++i;
                break;
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
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_SET:
            case OP_FAIL:
            default:
                *error = MICROERROR;
                return NULL;
        }
    }

    return writeto;
}

static uint8_t*
apply_map_set(bool (*validate_map)(const e::slice& data),
              const microop* op,
              uint8_t* writeto, microerror* error)
{
    assert(op->action == OP_SET);

    if (!validate_map(op->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    memmove(writeto, op->arg1.data(), op->arg1.size());
    return writeto + op->arg1.size();
}

uint8_t*
apply_map_microop(bool (*step_key)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                  bool (*step_val)(const uint8_t** ptr, const uint8_t* end, e::slice* elem),
                  bool (*validate_key)(const e::slice& elem),
                  bool (*validate_val)(const e::slice& elem),
                  int (*compare_key)(const e::slice& lhs, const e::slice& rhs),
                  uint8_t* (*write_key)(uint8_t* writeto, const e::slice& elem),
                  uint8_t* (*write_val)(uint8_t* writeto, const e::slice& elem),
                  uint8_t* (*apply_pod)(const e::slice& old_value,
                                        const microop* ops, size_t num_ops,
                                        uint8_t* writeto, microerror* error),
                  const e::slice& old_value,
                  microop* ops, size_t num_ops,
                  uint8_t* writeto, microerror* error)
{
    assert(num_ops > 0);

    if (!validate_key(ops->arg2) ||
        !validate_val(ops->arg1))
    {
        *error = MICROERROR;
        return NULL;
    }

    // Verify sorted order of the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        if (!validate_key(ops[i + 1].arg2) ||
            !validate_val(ops[i + 1].arg1))
        {
            *error = MICROERROR;
            return NULL;
        }
    }

    sort_microops_by_arg2(ops, ops + num_ops, compare_key);
    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        const uint8_t* next = ptr;
        e::slice key;
        e::slice val;

        if (!step_key(&next, end, &key))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_val(&next, end, &val))
        {
            *error = MICROERROR;
            return NULL;
        }

        int cmp = compare_key(key, ops[i].arg2);

        if (cmp < 0)
        {
            writeto = write_key(writeto, key);
            writeto = write_val(writeto, val);
            ptr = next;
        }
        else if (cmp > 0)
        {
            e::slice existing("", 0);
            writeto = write_key(writeto, ops[i].arg2);
            writeto = apply_pod(existing, ops + i, 1, writeto, error);

            if (!writeto)
            {
                return NULL;
            }

            ++i;
        }
        else
        {
            writeto = write_key(writeto, key);
            writeto = apply_pod(val, ops + i, 1, writeto, error);

            if (!writeto)
            {
                return NULL;
            }

            ptr = next;
            ++i;
        }
    }

    while (ptr < end)
    {
        e::slice key;
        e::slice val;

        if (!step_key(&ptr, end, &key))
        {
            *error = MICROERROR;
            return NULL;
        }

        if (!step_val(&ptr, end, &val))
        {
            *error = MICROERROR;
            return NULL;
        }

        writeto = write_key(writeto, key);
        writeto = write_val(writeto, val);
    }

    while (i < num_ops)
    {
        e::slice existing("", 0);
        writeto = write_key(writeto, ops[i].arg2);
        writeto = apply_pod(existing, ops + i, 1, writeto, error);

        if (!writeto)
        {
            return NULL;
        }

        ++i;
    }

    return writeto;
}

// This wrapper is needed because "apply_string operates on string attributes,
// which do not encode the size before the string because every attribute has an
// implicit "size" argument.  However, when applying the string to something in
// a map, we need to also encode the size.
uint8_t*
wrap_apply_string(const e::slice& old_value,
                  const microop* ops, size_t num_ops,
                  uint8_t* writeto, microerror* error)
{
    uint8_t* original_writeto = writeto;
    writeto = apply_string(old_value, ops, num_ops, writeto + sizeof(uint32_t), error);
    e::pack32le(static_cast<uint32_t>(writeto - original_writeto - sizeof(uint32_t)), original_writeto);
    return writeto;
}

#define APPLY_MAP(KEY_T, VAL_T, WRAP_PREFIX) \
    uint8_t* \
    apply_map_ ## KEY_T ## _ ## VAL_T(const e::slice& old_value, \
                           microop* ops, size_t num_ops, \
                           uint8_t* writeto, microerror* error) \
    { \
        assert(num_ops > 0); \
     \
        if (ops[0].action == OP_MAP_ADD || \
            ops[0].action == OP_MAP_REMOVE) \
        { \
            return apply_map_add_remove(step_ ## KEY_T, step_ ## VAL_T, \
                                        validate_as_ ## KEY_T, validate_as_ ## VAL_T, \
                                        compare_ ## KEY_T, \
                                        write_ ## KEY_T, write_ ## VAL_T, \
                                        old_value, ops, num_ops, writeto, error); \
        } \
        else if (ops[0].action == OP_SET && num_ops == 1) \
        { \
            return apply_map_set(validate_as_map_ ## KEY_T ## _ ## VAL_T, ops, writeto, error); \
        } \
        else \
        { \
            return apply_map_microop(step_ ## KEY_T, step_ ## VAL_T, \
                                     validate_as_ ## KEY_T, validate_as_ ## VAL_T, \
                                     compare_ ## KEY_T, \
                                     write_ ## KEY_T, write_ ## VAL_T, \
                                     WRAP_PREFIX ## VAL_T, \
                                     old_value, ops, num_ops, writeto, error); \
        } \
    }

APPLY_MAP(string, string, wrap_apply_)
APPLY_MAP(string, int64, apply_)
APPLY_MAP(string, float, apply_)
APPLY_MAP(int64, string, wrap_apply_)
APPLY_MAP(int64, int64, apply_)
APPLY_MAP(int64, float, apply_)
APPLY_MAP(float, string, wrap_apply_)
APPLY_MAP(float, int64, apply_)
APPLY_MAP(float, float, apply_)
