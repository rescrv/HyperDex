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

// C
#include <cstring>
#include <stdint.h>

// STL
#include <algorithm>

// e
#include <e/endian.h>

// HyperDaemon
#include "hyperdex.h"
#include "hyperdaemon/datatypes.h"

// The below functions often make use of a call to "validate" with the following
// prototype:
//
//     bool (*validate_TYPE)(const uint8_t** ptr, uint32_t* sz, const uint8_t* end)
//
// This returns true if it is able to decode a serialized object of type TYPE
// starting at the memory pointed to by *ptr.  It will never read at values >=
// end.  The 32-bit value "sz" is provided for use with other *_TYPE functions,
// and has a meaning dependent upon TYPE.  The value should be preserved for
// future calls into other *_TYPE functions, but it should not be interpreted
// for any meaning.
//
// On success, *ptr will be changed to point to one byte past the end of the
// structure decoded.

/////////////////////////////// String Primitives //////////////////////////////

static int
compare_string(const uint8_t* a_ptr, uint32_t a_sz,
               const uint8_t* b_ptr, uint32_t b_sz)
{
    int cmp = a_sz - b_sz;

    if (cmp == 0)
    {
        return memcmp(a_ptr + sizeof(uint32_t), b_ptr + sizeof(uint32_t), a_sz);
    }

    return cmp;
}

static int
compare_string_micros_arg1(const hyperdex::microop* lhs,
                           const hyperdex::microop* rhs)
{
    int cmp = lhs->argv1_string.size() - rhs->argv1_string.size();

    if (cmp == 0)
    {
        return memcmp(lhs->argv1_string.data(),
                      rhs->argv1_string.data(),
                      lhs->argv1_string.size());
    }

    return cmp;
}

static int
compare_string_micros_arg2(const hyperdex::microop* lhs,
                           const hyperdex::microop* rhs)
{
    int cmp = lhs->argv2_string.size() - rhs->argv2_string.size();

    if (cmp == 0)
    {
        return memcmp(lhs->argv2_string.data(),
                      rhs->argv2_string.data(),
                      lhs->argv2_string.size());
    }

    return cmp;
}

static int
compare_string_micro_arg1(const uint8_t* ptr, uint32_t ptr_sz,
                          const hyperdex::microop* op)
{
    int cmp = ptr_sz - op->argv1_string.size();

    if (cmp == 0)
    {
        return memcmp(ptr + sizeof(uint32_t), op->argv1_string.data(), ptr_sz);
    }

    return cmp;
}

static int
compare_string_micro_arg2(const uint8_t* ptr, uint32_t ptr_sz,
                          const hyperdex::microop* op)
{
    int cmp = ptr_sz - op->argv2_string.size();

    if (cmp == 0)
    {
        return memcmp(ptr + sizeof(uint32_t), op->argv2_string.data(), ptr_sz);
    }

    return cmp;
}

static uint8_t*
copy_string_from_serialized(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz)
{
    memmove(writeto, ptr, ptr_sz + sizeof(uint32_t));
    return writeto + ptr_sz + sizeof(uint32_t);
}

static uint8_t*
copy_string_from_micro_arg1(uint8_t* writeto, const hyperdex::microop* op)
{
    writeto = e::pack32le(op->argv1_string.size(), writeto);
    memmove(writeto, op->argv1_string.data(), op->argv1_string.size());
    return writeto + op->argv1_string.size();
}

static uint8_t*
copy_string_from_micro_arg2(uint8_t* writeto, const hyperdex::microop* op)
{
    writeto = e::pack32le(op->argv2_string.size(), writeto);
    memmove(writeto, op->argv2_string.data(), op->argv2_string.size());
    return writeto + op->argv2_string.size();
}

static e::slice
string_slice_from_serialized(const uint8_t* ptr, uint32_t ptr_sz, std::vector<uint8_t>*)
{
    return e::slice(ptr + sizeof(uint32_t), ptr_sz);
}

static bool
validate_string(const uint8_t** ptr, uint32_t* sz, const uint8_t* end)
{
    if (static_cast<size_t>(end - *ptr) < sizeof(uint32_t))
    {
        return false;
    }

    *ptr = e::unpack32le(*ptr, sz);
    *ptr += *sz;
    return *ptr <= end;
}

static uint8_t*
apply_string(const e::slice& old_value,
             const hyperdex::microop* ops,
             size_t num_ops,
             uint8_t* writeto,
             hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    e::slice base = old_value;
    bool set_base = false;
    const hyperdex::microop* prepend = NULL;
    const hyperdex::microop* append = NULL;

    for (size_t i = 0; i < num_ops; ++i)
    {
        switch (ops[i].action)
        {
            case OP_STRING_SET:

                if (set_base || prepend || append)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                base = ops[i].argv1_string;
                set_base = true;
                break;
            case OP_STRING_PREPEND:

                if (set_base || prepend)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                prepend = ops + i;
                break;
            case OP_STRING_APPEND:

                if (set_base || append)
                {
                    *error = NET_BADMICROS;
                    return NULL;
                }

                append = ops + i;
                break;
            case OP_INT64_SET:
            case OP_INT64_ADD:
            case OP_INT64_SUB:
            case OP_INT64_MUL:
            case OP_INT64_DIV:
            case OP_INT64_MOD:
            case OP_INT64_AND:
            case OP_INT64_OR:
            case OP_INT64_XOR:
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    if (prepend)
    {
        memmove(writeto, prepend->argv1_string.data(), prepend->argv1_string.size());
        writeto += prepend->argv1_string.size();
    }

    memmove(writeto, base.data(), base.size());
    writeto += base.size();

    if (append)
    {
        memmove(writeto, append->argv1_string.data(), append->argv1_string.size());
        writeto += append->argv1_string.size();
    }

    return writeto;
}

/////////////////////////////// Int64 Primitives ///////////////////////////////

static int
compare_int64(const uint8_t* a_ptr, uint32_t a_sz,
              const uint8_t* b_ptr, uint32_t b_sz)
{
    assert(a_sz == sizeof(int64_t));
    assert(b_sz == sizeof(int64_t));
    int64_t a;
    int64_t b;
    e::unpack64le(a_ptr, &a);
    e::unpack64le(b_ptr, &b);
    return a - b;
}

static int
compare_int64_micros_arg1(const hyperdex::microop* lhs,
                          const hyperdex::microop* rhs)
{
    return lhs->argv1_int64 - rhs->argv1_int64;
}

static int
compare_int64_micros_arg2(const hyperdex::microop* lhs,
                          const hyperdex::microop* rhs)
{
    return lhs->argv2_int64 - rhs->argv2_int64;
}

static int
compare_int64_micro_arg1(const uint8_t* ptr, uint32_t ptr_sz,
                         const hyperdex::microop* op)
{
    assert(ptr_sz == sizeof(int64_t));
    int64_t number;
    e::unpack64le(ptr, &number);

    return number - op->argv1_int64;
}

static int
compare_int64_micro_arg2(const uint8_t* ptr, uint32_t ptr_sz,
                         const hyperdex::microop* op)
{
    assert(ptr_sz == sizeof(int64_t));
    int64_t number;
    e::unpack64le(ptr, &number);

    return number - op->argv2_int64;
}

static uint8_t*
copy_int64_from_serialized(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz)
{
    assert(ptr_sz == sizeof(int64_t));
    memmove(writeto, ptr, sizeof(int64_t));
    return writeto + sizeof(int64_t);
}

static uint8_t*
copy_int64_from_micro_arg1(uint8_t* writeto, const hyperdex::microop* op)
{
    return e::pack64le(op->argv1_int64, writeto);
}

static uint8_t*
copy_int64_from_micro_arg2(uint8_t* writeto, const hyperdex::microop* op)
{
    return e::pack64le(op->argv2_int64, writeto);
}

static e::slice
int64_slice_from_serialized(const uint8_t* ptr, uint32_t ptr_sz, std::vector<uint8_t>*)
{
    return e::slice(ptr, ptr_sz);
}

static bool
validate_int64(const uint8_t** ptr, uint32_t* sz, const uint8_t* end)
{
    uint32_t tmp = end - *ptr;

    if (tmp < sizeof(int64_t))
    {
        return false;
    }

    *sz = sizeof(int64_t);
    *ptr += sizeof(int64_t);
    return true;
}

static uint8_t*
apply_int64(const e::slice& old_value,
            const hyperdex::microop* ops,
            size_t num_ops,
            uint8_t* writeto,
            hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    int64_t number = 0;

    if (old_value.size())
    {
        e::unpack64le(old_value.data(), &number);
    }

    for (size_t i = 0; i < num_ops; ++i)
    {
        const hyperdex::microop* op = ops + i;

        switch (op->action)
        {
            case OP_INT64_SET:
                number = op->argv1_int64;
                break;
            case OP_INT64_ADD:
                number += op->argv1_int64;
                break;
            case OP_INT64_SUB:
                number -= op->argv1_int64;
                break;
            case OP_INT64_MUL:
                number *= op->argv1_int64;
                break;
            case OP_INT64_DIV:
                number /= op->argv1_int64;
                break;
            case OP_INT64_MOD:
                number %= op->argv1_int64;
                break;
            case OP_INT64_AND:
                number &= op->argv1_int64;
                break;
            case OP_INT64_OR:
                number |= op->argv1_int64;
                break;
            case OP_INT64_XOR:
                number ^= op->argv1_int64;
                break;
            case OP_STRING_SET:
            case OP_STRING_APPEND:
            case OP_STRING_PREPEND:
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    return e::pack64le(number, writeto);
}

//////////////////////////// Abstract List Functions ///////////////////////////

static bool
validate_list(bool (*validate_elem)(const uint8_t** ptr,
                                    uint32_t* ptr_sz,
                                    const uint8_t* end),
              const e::slice& list)
{
    const uint8_t* ptr = list.data();
    const uint8_t* const end = list.data() + list.size();

    while (ptr < end)
    {
        uint32_t sz;

        if (!validate_elem(&ptr, &sz, end))
        {
            return false;
        }
    }

    return ptr == end;
}

static uint8_t*
apply_list(uint8_t* (*copy_elem_from_micro)(uint8_t* writeto,
                                            const hyperdex::microop* op),
           const e::slice& old_value,
           const hyperdex::microop* ops,
           size_t num_ops,
           uint8_t* writeto,
           hyperdex::network_returncode* error)
{
    // Apply the lpush ops
    for (ssize_t i = num_ops - 1; i >= 0; --i)
    {
        if (ops->action != hyperdex::OP_LIST_LPUSH)
        {
            continue;
        }

        writeto = copy_elem_from_micro(writeto, ops + i);
    }

    // Copy the middle of the list
    memmove(writeto, old_value.data(), old_value.size());
    writeto += old_value.size();

    // Apply the rpush ops
    for (size_t i = 0; i < num_ops; ++i)
    {
        if (ops->action != hyperdex::OP_LIST_RPUSH)
        {
            if (ops->action != hyperdex::OP_LIST_LPUSH)
            {
                *error = hyperdex::NET_BADMICROS;
                return NULL;
            }

            continue;
        }

        writeto = copy_elem_from_micro(writeto, ops + i);
    }

    return writeto;
}

//////////////////////////// Abstract Set Functions ////////////////////////////

static bool
validate_set(bool (*validate_elem)(const uint8_t** ptr,
                                   uint32_t* ptr_sz,
                                   const uint8_t* end),
             int (*compare_elem)(const uint8_t* a_ptr, uint32_t a_sz,
                                 const uint8_t* b_ptr, uint32_t b_sz),
             const e::slice& set)
{
    bool has_old = false;
    uint32_t old_sz = 0;
    const uint8_t* old_ptr = NULL;
    const uint8_t* ptr = set.data();
    const uint8_t* const end = set.data() + set.size();

    while (ptr < end)
    {
        uint32_t sz;
        const uint8_t* next_ptr = ptr;

        if (!validate_elem(&next_ptr, &sz, end))
        {
            return false;
        }

        // sz, ptr are size and pointer to the element we just validated.
        // old_sz, old_ptr are size and pointer to the previous element
        // next_ptr is the start of the next element

        if (has_old)
        {
            int cmp = compare_elem(old_ptr, old_sz, ptr, sz);

            if (cmp >= 0)
            {
                return false;
            }
        }

        old_sz = sz;
        old_ptr = ptr;
        has_old = true;
        ptr = next_ptr;
    }

    return ptr == end;
}

static uint8_t*
apply_set_add_remove(bool (*validate_elem)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                     int (*compare_micros)(const hyperdex::microop* lhs, const hyperdex::microop* rhs),
                     int (*compare_elem)(const uint8_t* ptr, uint32_t ptr_sz, const hyperdex::microop* op),
                     uint8_t* (*copy_elem_from_set)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                     uint8_t* (*copy_elem_from_micro)(uint8_t* writeto, const hyperdex::microop* op),
                     const e::slice& old_value,
                     const hyperdex::microop* ops,
                     size_t num_ops,
                     uint8_t* writeto,
                     hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    // Verify sorted order of the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        if (compare_micros(ops + i, ops + i + 1) >= 0)
        {
            *error = NET_BADMICROS;
            return NULL;
        }
    }

    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* next_from_set = ptr;

        if (!validate_elem(&next_from_set, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        int cmp = compare_elem(ptr, ptr_sz, ops + i);

        if (cmp < 0)
        {
            writeto = copy_elem_from_set(writeto, ptr, ptr_sz);
            ptr = next_from_set;
        }
        else if (cmp > 0)
        {
            switch (ops[i].action)
            {
                case OP_SET_ADD:
                    writeto = copy_elem_from_micro(writeto, ops + i);
                    ++i;
                    break;
                case OP_SET_REMOVE:
                    // Nothing to remove
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
                case OP_STRING_SET:
                case OP_STRING_APPEND:
                case OP_STRING_PREPEND:
                case OP_INT64_SET:
                case OP_INT64_ADD:
                case OP_INT64_SUB:
                case OP_INT64_MUL:
                case OP_INT64_DIV:
                case OP_INT64_MOD:
                case OP_INT64_AND:
                case OP_INT64_OR:
                case OP_INT64_XOR:
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_MAP_ADD:
                case OP_MAP_REMOVE:
                case OP_FAIL:
                default:
                    *error = NET_BADMICROS;
                    return NULL;
            }
        }
        else
        {
            switch (ops[i].action)
            {
                case OP_SET_ADD:
                    writeto = copy_elem_from_micro(writeto, ops + i);
                    ptr = next_from_set;
                    ++i;
                    break;
                case OP_SET_REMOVE:
                    ptr = next_from_set;
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
                case OP_STRING_SET:
                case OP_STRING_APPEND:
                case OP_STRING_PREPEND:
                case OP_INT64_SET:
                case OP_INT64_ADD:
                case OP_INT64_SUB:
                case OP_INT64_MUL:
                case OP_INT64_DIV:
                case OP_INT64_MOD:
                case OP_INT64_AND:
                case OP_INT64_OR:
                case OP_INT64_XOR:
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_MAP_ADD:
                case OP_MAP_REMOVE:
                case OP_FAIL:
                default:
                    *error = NET_BADMICROS;
                    return NULL;
            }
        }
    }

    while (ptr < end)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* next_from_set = ptr;

        if (!validate_elem(&next_from_set, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        writeto = copy_elem_from_set(writeto, ptr, ptr_sz);
        ptr = next_from_set;
    }

    while (i < num_ops)
    {
        switch (ops[i].action)
        {
            case OP_SET_ADD:
                writeto = copy_elem_from_micro(writeto, ops + i);
                ++i;
                break;
            case OP_SET_REMOVE:
                ++i;
                break;
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_STRING_SET:
            case OP_STRING_APPEND:
            case OP_STRING_PREPEND:
            case OP_INT64_SET:
            case OP_INT64_ADD:
            case OP_INT64_SUB:
            case OP_INT64_MUL:
            case OP_INT64_DIV:
            case OP_INT64_MOD:
            case OP_INT64_AND:
            case OP_INT64_OR:
            case OP_INT64_XOR:
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    return writeto;
}

static uint8_t*
apply_set_intersect(bool (*validate_set)(const e::slice& data),
                    bool (*validate_elem)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                    int (*compare_elem)(const uint8_t* a_ptr, uint32_t a_sz,
                                        const uint8_t* b_ptr, uint32_t b_sz),
                    uint8_t* (*copy_elem_from_set)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                    const e::slice& old_value,
                    const hyperdex::microop* ops,
                    size_t num_ops,
                    uint8_t* writeto,
                    hyperdex::network_returncode* error)
{
    // There's an efficiency tradeoff in this function.  We could validate that
    // the user-provided set is sorted, and then run a merge algorithm, or we
    // could validate that it's sorted on the fly.  It's easier to pre-validate
    // that the set is sorted, and the efficiency trade-off is on the order of
    // constants.
    using namespace hyperdex;

    if (num_ops != 1)
    {
        *error = NET_BADMICROS;
        return NULL;
    }

    assert(ops->action == hyperdex::OP_SET_INTERSECT);

    if (!validate_set(e::slice(ops->argv1_string)))
    {
        *error = NET_BADMICROS;
        return NULL;
    }

    const uint8_t* set_ptr = old_value.data();
    const uint8_t* const set_end = old_value.data() + old_value.size();
    const uint8_t* new_ptr = ops->argv1_string.data();
    const uint8_t* const new_end = ops->argv1_string.data() + ops->argv1_string.size();

    // We only need to consider this case.
    while (set_ptr < set_end && new_ptr < new_end)
    {
        uint32_t set_sz = 0;
        uint32_t new_sz = 0;
        const uint8_t* next_set_ptr = set_ptr;
        const uint8_t* next_new_ptr = new_ptr;

        if (!validate_elem(&next_set_ptr, &set_sz, set_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        if (!validate_elem(&next_new_ptr, &new_sz, new_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        int cmp = compare_elem(set_ptr, set_sz, new_ptr, new_sz);

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
            writeto = copy_elem_from_set(writeto, set_ptr, new_sz);
            set_ptr = next_set_ptr;
            new_ptr = next_new_ptr;
        }
    }

    return writeto;
}

static uint8_t*
apply_set_union(bool (*validate_set)(const e::slice& data),
                bool (*validate_elem)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                int (*compare_elem)(const uint8_t* a_ptr, uint32_t a_sz,
                                    const uint8_t* b_ptr, uint32_t b_sz),
                uint8_t* (*copy_elem_from_set)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                const e::slice& old_value,
                const hyperdex::microop* ops,
                size_t num_ops,
                uint8_t* writeto,
                hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    if (num_ops != 1)
    {
        *error = NET_BADMICROS;
        return NULL;
    }

    assert(ops->action == hyperdex::OP_SET_UNION);

    if (!validate_set(e::slice(ops->argv1_string)))
    {
        *error = NET_BADMICROS;
        return NULL;
    }

    const uint8_t* set_ptr = old_value.data();
    const uint8_t* const set_end = old_value.data() + old_value.size();
    const uint8_t* new_ptr = ops->argv1_string.data();
    const uint8_t* const new_end = ops->argv1_string.data() + ops->argv1_string.size();

    while (set_ptr < set_end && new_ptr < new_end)
    {
        uint32_t set_sz = 0;
        uint32_t new_sz = 0;
        const uint8_t* next_set_ptr = set_ptr;
        const uint8_t* next_new_ptr = new_ptr;

        if (!validate_elem(&next_set_ptr, &set_sz, set_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        if (!validate_elem(&next_new_ptr, &new_sz, new_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        int cmp = compare_elem(set_ptr, set_sz, new_ptr, new_sz);

        if (cmp < 0)
        {
            writeto = copy_elem_from_set(writeto, set_ptr, set_sz);
            set_ptr = next_set_ptr;
        }
        else if (cmp > 0)
        {
            writeto = copy_elem_from_set(writeto, new_ptr, new_sz);
            new_ptr = next_new_ptr;
        }
        else
        {
            writeto = copy_elem_from_set(writeto, set_ptr, set_sz);
            set_ptr = next_set_ptr;
            new_ptr = next_new_ptr;
        }
    }

    while (set_ptr < set_end)
    {
        uint32_t set_sz = 0;
        const uint8_t* next_set_ptr = set_ptr;

        if (!validate_elem(&next_set_ptr, &set_sz, set_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        writeto = copy_elem_from_set(writeto, set_ptr, set_sz);
        set_ptr = next_set_ptr;
    }

    while (new_ptr < new_end)
    {
        uint32_t new_sz = 0;
        const uint8_t* next_new_ptr = new_ptr;

        if (!validate_elem(&next_new_ptr, &new_sz, new_end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        writeto = copy_elem_from_set(writeto, new_ptr, new_sz);
        new_ptr = next_new_ptr;
    }

    return writeto;
}

//////////////////////////// Abstract Map Functions ////////////////////////////

static bool
validate_map(bool (*validate_key)(const uint8_t** ptr,
                                  uint32_t* ptr_sz,
                                  const uint8_t* end),
             bool (*validate_value)(const uint8_t** ptr,
                                    uint32_t* ptr_sz,
                                    const uint8_t* end),
             int (*compare_key)(const uint8_t* a_ptr, uint32_t a_sz,
                                const uint8_t* b_ptr, uint32_t b_sz),
             const e::slice& map)
{
    bool has_old = false;
    uint32_t old_sz = 0;
    const uint8_t* old_ptr = NULL;
    const uint8_t* ptr = map.data();
    const uint8_t* const end = map.data() + map.size();
    uint32_t dummy;

    while (ptr < end)
    {
        uint32_t sz;
        const uint8_t* next_ptr = ptr;

        if (!validate_key(&next_ptr, &sz, end))
        {
            return false;
        }

        if (!validate_value(&next_ptr, &dummy, end))
        {
            return false;
        }

        // sz, ptr are size and pointer to the key we just validated.
        // old_sz, old_ptr are size and pointer to the previous key
        // next_ptr is the start of the next key
        // the value associated with the current key is valid

        if (has_old)
        {
            int cmp = compare_key(old_ptr, old_sz, ptr, sz);

            if (cmp >= 0)
            {
                return false;
            }
        }

        old_sz = sz;
        old_ptr = ptr;
        has_old = true;
        ptr = next_ptr;
    }

    return ptr == end;
}

static uint8_t*
apply_map_add_remove(bool (*validate_key)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                     bool (*validate_val)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                     int (*compare_micros)(const hyperdex::microop* lhs, const hyperdex::microop* rhs),
                     int (*compare_elem)(const uint8_t* ptr, uint32_t ptr_sz, const hyperdex::microop* op),
                     uint8_t* (*copy_key_from_map)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                     uint8_t* (*copy_key_from_micro)(uint8_t* writeto, const hyperdex::microop* op),
                     uint8_t* (*copy_val_from_map)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                     uint8_t* (*copy_val_from_micro)(uint8_t* writeto, const hyperdex::microop* op),
                     const e::slice& old_value,
                     const hyperdex::microop* ops,
                     size_t num_ops,
                     uint8_t* writeto,
                     hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    // Verify sorted order of the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        if (compare_micros(ops + i, ops + i + 1) >= 0)
        {
            *error = NET_BADMICROS;
            return NULL;
        }
    }

    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* val_ptr = ptr;

        if (!validate_key(&val_ptr, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        uint32_t val_ptr_sz = 0;
        const uint8_t* next_from_map = val_ptr;

        if (!validate_val(&next_from_map, &val_ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        int cmp = compare_elem(ptr, ptr_sz, ops + i);

        if (cmp < 0)
        {
            writeto = copy_key_from_map(writeto, ptr, ptr_sz);
            writeto = copy_val_from_map(writeto, val_ptr, val_ptr_sz);
            ptr = next_from_map;
        }
        else if (cmp > 0)
        {
            switch (ops[i].action)
            {
                case OP_MAP_ADD:
                    writeto = copy_key_from_micro(writeto, ops + i);
                    writeto = copy_val_from_micro(writeto, ops + i);
                    ++i;
                    break;
                case OP_MAP_REMOVE:
                    // Nothing to remove
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
                case OP_STRING_SET:
                case OP_STRING_APPEND:
                case OP_STRING_PREPEND:
                case OP_INT64_SET:
                case OP_INT64_ADD:
                case OP_INT64_SUB:
                case OP_INT64_MUL:
                case OP_INT64_DIV:
                case OP_INT64_MOD:
                case OP_INT64_AND:
                case OP_INT64_OR:
                case OP_INT64_XOR:
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_SET_ADD:
                case OP_SET_REMOVE:
                case OP_FAIL:
                default:
                    *error = NET_BADMICROS;
                    return NULL;
            }
        }
        else
        {
            switch (ops[i].action)
            {
                case OP_MAP_ADD:
                    writeto = copy_key_from_micro(writeto, ops + i);
                    writeto = copy_val_from_micro(writeto, ops + i);
                    ptr = next_from_map;
                    ++i;
                    break;
                case OP_MAP_REMOVE:
                    ptr = next_from_map;
                    ++i;
                    break;
                case OP_SET_INTERSECT:
                case OP_SET_UNION:
                case OP_STRING_SET:
                case OP_STRING_APPEND:
                case OP_STRING_PREPEND:
                case OP_INT64_SET:
                case OP_INT64_ADD:
                case OP_INT64_SUB:
                case OP_INT64_MUL:
                case OP_INT64_DIV:
                case OP_INT64_MOD:
                case OP_INT64_AND:
                case OP_INT64_OR:
                case OP_INT64_XOR:
                case OP_LIST_LPUSH:
                case OP_LIST_RPUSH:
                case OP_SET_ADD:
                case OP_SET_REMOVE:
                case OP_FAIL:
                default:
                    *error = NET_BADMICROS;
                    return NULL;
            }
        }
    }

    while (ptr < end)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* val_ptr = ptr;

        if (!validate_key(&val_ptr, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        uint32_t val_ptr_sz = 0;
        const uint8_t* next_from_map = val_ptr;

        if (!validate_val(&next_from_map, &val_ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        writeto = copy_key_from_map(writeto, ptr, ptr_sz);
        writeto = copy_val_from_map(writeto, val_ptr, val_ptr_sz);
        ptr = next_from_map;
    }

    while (i < num_ops)
    {
        switch (ops[i].action)
        {
            case OP_MAP_ADD:
                writeto = copy_key_from_micro(writeto, ops + i);
                writeto = copy_val_from_micro(writeto, ops + i);
                ++i;
                break;
            case OP_MAP_REMOVE:
                ++i;
                break;
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_STRING_SET:
            case OP_STRING_APPEND:
            case OP_STRING_PREPEND:
            case OP_INT64_SET:
            case OP_INT64_ADD:
            case OP_INT64_SUB:
            case OP_INT64_MUL:
            case OP_INT64_DIV:
            case OP_INT64_MOD:
            case OP_INT64_AND:
            case OP_INT64_OR:
            case OP_INT64_XOR:
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    return writeto;
}

static uint8_t*
apply_map_microop(bool (*validate_key)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                  bool (*validate_val)(const uint8_t** ptr, uint32_t* ptr_sz, const uint8_t* end),
                  int (*compare_micros)(const hyperdex::microop* lhs, const hyperdex::microop* rhs),
                  int (*compare_elem)(const uint8_t* ptr, uint32_t ptr_sz, const hyperdex::microop* op),
                  uint8_t* (*copy_key_from_map)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                  uint8_t* (*copy_key_from_micro)(uint8_t* writeto, const hyperdex::microop* op),
                  uint8_t* (*copy_val_from_map)(uint8_t* writeto, const uint8_t* ptr, uint32_t ptr_sz),
                  e::slice (*slice_from_map)(const uint8_t* ptr, uint32_t ptr_sz, std::vector<uint8_t>* tmp),
                  uint8_t* (*apply_pod)(const e::slice& old_value,
                                        const hyperdex::microop* ops,
                                        size_t num_ops,
                                        uint8_t* writeto,
                                        hyperdex::network_returncode* error),
                  const e::slice& old_value,
                  const hyperdex::microop* ops,
                  size_t num_ops,
                  uint8_t* writeto,
                  hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    // Verify sorted order of the microops.
    for (size_t i = 0; i < num_ops - 1; ++i)
    {
        // The '=' part of '>=' ensures that we don't need to do anything fancy
        // to accommodate multiple micro ops on the same map key.  We're always
        // guaranteed to be operating on the same object attribute, so we need
        // no special consideration for that.
        //
        // XXX Modify this function to allow multiple micro ops per map key.
        if (compare_micros(ops + i, ops + i + 1) >= 0)
        {
            *error = NET_BADMICROS;
            return NULL;
        }
    }

    const uint8_t* ptr = old_value.data();
    const uint8_t* const end = old_value.data() + old_value.size();
    size_t i = 0;

    while (ptr < end && i < num_ops)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* val_ptr = ptr;

        if (!validate_key(&val_ptr, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        uint32_t val_ptr_sz = 0;
        const uint8_t* next_from_map = val_ptr;

        if (!validate_val(&next_from_map, &val_ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        int cmp = compare_elem(ptr, ptr_sz, ops + i);

        if (cmp < 0)
        {
            writeto = copy_key_from_map(writeto, ptr, ptr_sz);
            writeto = copy_val_from_map(writeto, val_ptr, val_ptr_sz);
            ptr = next_from_map;
        }
        else if (cmp > 0)
        {
            e::slice existing("", 0);
            writeto = copy_key_from_micro(writeto, ops + i);
            writeto = apply_pod(existing, ops + i, 1, writeto, error);

            if (!writeto)
            {
                return NULL;
            }

            ++i;
        }
        else
        {
            std::vector<uint8_t> tmp;
            e::slice existing = slice_from_map(val_ptr, val_ptr_sz, &tmp);
            writeto = copy_key_from_micro(writeto, ops + i);
            writeto = apply_pod(existing, ops + i, 1, writeto, error);

            if (!writeto)
            {
                return NULL;
            }

            ptr = next_from_map;
            ++i;
        }
    }

    while (ptr < end)
    {
        uint32_t ptr_sz = 0;
        const uint8_t* val_ptr = ptr;

        if (!validate_key(&val_ptr, &ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        uint32_t val_ptr_sz = 0;
        const uint8_t* next_from_map = val_ptr;

        if (!validate_val(&next_from_map, &val_ptr_sz, end))
        {
            *error = NET_BADMICROS;
            return NULL;
        }

        writeto = copy_key_from_map(writeto, ptr, ptr_sz);
        writeto = copy_val_from_map(writeto, val_ptr, val_ptr_sz);
        ptr = next_from_map;
    }

    while (i < num_ops)
    {
        e::slice existing("", 0);
        writeto = copy_key_from_micro(writeto, ops + i);
        writeto = apply_pod(existing, ops + i, 1, writeto, error);

        if (!writeto)
        {
            return NULL;
        }

        ++i;
    }

    return writeto;
}

////////////////////////// Instantiated List Functions /////////////////////////

static bool
validate_list_string(const e::slice& list)
{
    return validate_list(validate_string, list);
}

static bool
validate_list_int64(const e::slice& list)
{
    return validate_list(validate_int64, list);
}

static uint8_t*
apply_list_string(const e::slice& old_value,
                  const hyperdex::microop* ops,
                  size_t num_ops,
                  uint8_t* writeto,
                  hyperdex::network_returncode* error)
{
    return apply_list(copy_string_from_micro_arg1,
                      old_value, ops, num_ops, writeto, error);
}

static uint8_t*
apply_list_int64(const e::slice& old_value,
                 const hyperdex::microop* ops,
                 size_t num_ops,
                 uint8_t* writeto,
                 hyperdex::network_returncode* error)
{
    return apply_list(copy_int64_from_micro_arg1,
                      old_value, ops, num_ops, writeto, error);
}

////////////////////////// Instantiated Set Functions //////////////////////////

static bool
validate_set_string(const e::slice& set)
{
    return validate_set(validate_string, compare_string, set);
}

static bool
validate_set_int64(const e::slice& set)
{
    return validate_set(validate_int64, compare_int64, set);
}

static uint8_t*
apply_set_string(const e::slice& old_value,
                 const hyperdex::microop* ops,
                 size_t num_ops,
                 uint8_t* writeto,
                 hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_SET_ADD ||
        ops[0].action == hyperdex::OP_SET_REMOVE)
    {
        return apply_set_add_remove(validate_string,
                                    compare_string_micros_arg1,
                                    compare_string_micro_arg1,
                                    copy_string_from_serialized,
                                    copy_string_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else if (ops[0].action == hyperdex::OP_SET_INTERSECT)
    {
        return apply_set_intersect(validate_set_string, validate_string,
                                   compare_string, copy_string_from_serialized,
                                   old_value, ops, num_ops, writeto, error);
    }
    else if (ops[0].action == hyperdex::OP_SET_UNION)
    {
        return apply_set_union(validate_set_string, validate_string,
                               compare_string, copy_string_from_serialized,
                               old_value, ops, num_ops, writeto, error);
    }
    else
    {
        *error = hyperdex::NET_BADMICROS;
        return NULL;
    }
}

static uint8_t*
apply_set_int64(const e::slice& old_value,
                const hyperdex::microop* ops,
                size_t num_ops,
                uint8_t* writeto,
                hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_SET_ADD ||
        ops[0].action == hyperdex::OP_SET_REMOVE)
    {
        return apply_set_add_remove(validate_int64,
                                    compare_int64_micros_arg1,
                                    compare_int64_micro_arg1,
                                    copy_int64_from_serialized,
                                    copy_int64_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else if (ops[0].action == hyperdex::OP_SET_INTERSECT)
    {
        return apply_set_intersect(validate_set_int64, validate_int64,
                                   compare_int64, copy_int64_from_serialized,
                                   old_value, ops, num_ops, writeto, error);
    }
    else if (ops[0].action == hyperdex::OP_SET_UNION)
    {
        return apply_set_union(validate_set_int64, validate_int64,
                               compare_int64, copy_int64_from_serialized,
                               old_value, ops, num_ops, writeto, error);
    }
    else
    {
        *error = hyperdex::NET_BADMICROS;
        return NULL;
    }
}

////////////////////////// Instantiated Map Functions //////////////////////////

static bool
validate_map_string_string(const e::slice& map)
{
    return validate_map(validate_string, validate_string, compare_string, map);
}

static bool
validate_map_string_int64(const e::slice& map)
{
    return validate_map(validate_string, validate_int64, compare_string, map);
}

static bool
validate_map_int64_string(const e::slice& map)
{
    return validate_map(validate_int64, validate_string, compare_int64, map);
}

static bool
validate_map_int64_int64(const e::slice& map)
{
    return validate_map(validate_int64, validate_int64, compare_int64, map);
}

// This wrapper is needed because "apply_string operates on string attributes,
// which do not encode the size before the string because every attribute has an
// implicit "size" argument.  However, when applying the string to something in
// a map, we need to also encode the size.
static uint8_t*
apply_string_wrapper(const e::slice& old_value,
                     const hyperdex::microop* ops,
                     size_t num_ops,
                     uint8_t* writeto,
                     hyperdex::network_returncode* error)
{
    uint8_t* original_writeto = writeto;
    writeto = apply_string(old_value, ops, num_ops, writeto + sizeof(uint32_t), error);
    e::pack32le(writeto - original_writeto - sizeof(uint32_t), original_writeto);
    return writeto;
}

static uint8_t*
apply_map_string_string(const e::slice& old_value,
                        const hyperdex::microop* ops,
                        size_t num_ops,
                        uint8_t* writeto,
                        hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_MAP_ADD ||
        ops[0].action == hyperdex::OP_MAP_REMOVE)
    {
        return apply_map_add_remove(validate_string,
                                    validate_string,
                                    compare_string_micros_arg2,
                                    compare_string_micro_arg2,
                                    copy_string_from_serialized,
                                    copy_string_from_micro_arg2,
                                    copy_string_from_serialized,
                                    copy_string_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else
    {
        return apply_map_microop(validate_string,
                                 validate_string,
                                 compare_string_micros_arg2,
                                 compare_string_micro_arg2,
                                 copy_string_from_serialized,
                                 copy_string_from_micro_arg2,
                                 copy_string_from_serialized,
                                 string_slice_from_serialized,
                                 apply_string_wrapper,
                                 old_value, ops, num_ops, writeto, error);
    }
}

static uint8_t*
apply_map_string_int64(const e::slice& old_value,
                       const hyperdex::microop* ops,
                       size_t num_ops,
                       uint8_t* writeto,
                       hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_MAP_ADD ||
        ops[0].action == hyperdex::OP_MAP_REMOVE)
    {
        return apply_map_add_remove(validate_string,
                                    validate_int64,
                                    compare_string_micros_arg2,
                                    compare_string_micro_arg2,
                                    copy_string_from_serialized,
                                    copy_string_from_micro_arg2,
                                    copy_int64_from_serialized,
                                    copy_int64_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else
    {
        return apply_map_microop(validate_string,
                                 validate_int64,
                                 compare_string_micros_arg2,
                                 compare_string_micro_arg2,
                                 copy_string_from_serialized,
                                 copy_string_from_micro_arg2,
                                 copy_int64_from_serialized,
                                 int64_slice_from_serialized,
                                 apply_int64,
                                 old_value, ops, num_ops, writeto, error);
    }
}

static uint8_t*
apply_map_int64_string(const e::slice& old_value,
                       const hyperdex::microop* ops,
                       size_t num_ops,
                       uint8_t* writeto,
                       hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_MAP_ADD ||
        ops[0].action == hyperdex::OP_MAP_REMOVE)
    {
        return apply_map_add_remove(validate_int64,
                                    validate_string,
                                    compare_int64_micros_arg2,
                                    compare_int64_micro_arg2,
                                    copy_int64_from_serialized,
                                    copy_int64_from_micro_arg2,
                                    copy_string_from_serialized,
                                    copy_string_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else
    {
        return apply_map_microop(validate_int64,
                                 validate_string,
                                 compare_int64_micros_arg2,
                                 compare_int64_micro_arg2,
                                 copy_int64_from_serialized,
                                 copy_int64_from_micro_arg2,
                                 copy_string_from_serialized,
                                 string_slice_from_serialized,
                                 apply_string_wrapper,
                                 old_value, ops, num_ops, writeto, error);
    }
}

static uint8_t*
apply_map_int64_int64(const e::slice& old_value,
                      const hyperdex::microop* ops,
                      size_t num_ops,
                      uint8_t* writeto,
                      hyperdex::network_returncode* error)
{
    assert(num_ops > 0);

    if (ops[0].action == hyperdex::OP_MAP_ADD ||
        ops[0].action == hyperdex::OP_MAP_REMOVE)
    {
        return apply_map_add_remove(validate_int64,
                                    validate_int64,
                                    compare_int64_micros_arg2,
                                    compare_int64_micro_arg2,
                                    copy_int64_from_serialized,
                                    copy_int64_from_micro_arg2,
                                    copy_int64_from_serialized,
                                    copy_int64_from_micro_arg1,
                                    old_value, ops, num_ops, writeto, error);
    }
    else
    {
        return apply_map_microop(validate_int64,
                                 validate_int64,
                                 compare_int64_micros_arg2,
                                 compare_int64_micro_arg2,
                                 copy_int64_from_serialized,
                                 copy_int64_from_micro_arg2,
                                 copy_int64_from_serialized,
                                 int64_slice_from_serialized,
                                 apply_int64,
                                 old_value, ops, num_ops, writeto, error);
    }
}

/////////////////////////////// Public Functions ///////////////////////////////

bool
hyperdaemon :: validate_datatype(hyperdatatype datatype, const e::slice& data)
{
    switch (datatype)
    {
        case HYPERDATATYPE_STRING:
            return true;
        case HYPERDATATYPE_INT64:
            return data.size() == 0 || data.size() == sizeof(int64_t);
        case HYPERDATATYPE_LIST_STRING:
            return validate_list_string(data);
        case HYPERDATATYPE_LIST_INT64:
            return validate_list_int64(data);
        case HYPERDATATYPE_SET_STRING:
            return validate_set_string(data);
        case HYPERDATATYPE_SET_INT64:
            return validate_set_int64(data);
        case HYPERDATATYPE_MAP_STRING_STRING:
            return validate_map_string_string(data);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return validate_map_string_int64(data);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return validate_map_int64_string(data);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return validate_map_int64_int64(data);
        case HYPERDATATYPE_GARBAGE:
            return false;
        default:
            abort();
    }
}

size_t
hyperdaemon :: sizeof_microop(const hyperdex::microop& op)
{
    using namespace hyperdex;

    switch (op.action)
    {
        case OP_FAIL:
            return 0;
        case OP_STRING_SET:
        case OP_STRING_APPEND:
        case OP_STRING_PREPEND:
            return sizeof(uint32_t) + op.argv1_string.size();
        case OP_INT64_SET:
        case OP_INT64_ADD:
        case OP_INT64_SUB:
        case OP_INT64_MUL:
        case OP_INT64_DIV:
        case OP_INT64_MOD:
        case OP_INT64_AND:
        case OP_INT64_OR:
        case OP_INT64_XOR:
            return sizeof(int64_t);
        case OP_LIST_LPUSH:
        case OP_LIST_RPUSH:

            if (op.type == HYPERDATATYPE_LIST_STRING)
            {
                return sizeof(int32_t) + op.argv1_string.size();
            }
            else if (op.type == HYPERDATATYPE_LIST_INT64)
            {
                return sizeof(int64_t);
            }

            return 0;
        case OP_SET_ADD:

            if (op.type == HYPERDATATYPE_SET_STRING)
            {
                return sizeof(int32_t) + op.argv1_string.size();
            }
            else if (op.type == HYPERDATATYPE_SET_INT64)
            {
                return sizeof(int64_t);
            }

            return 0;
        case OP_SET_REMOVE:
            return 0;
        case OP_SET_INTERSECT:
            return 0;
        case OP_SET_UNION:
            return op.argv1_string.size();
        case OP_MAP_ADD:

            if (op.type == HYPERDATATYPE_MAP_STRING_STRING)
            {
                return 2 * sizeof(int32_t) + op.argv2_string.size()
                       + op.argv1_string.size();
            }
            else if (op.type == HYPERDATATYPE_MAP_STRING_INT64)
            {
                return sizeof(int32_t) + op.argv2_string.size() + sizeof(int64_t);
            }
            else if (op.type == HYPERDATATYPE_MAP_INT64_STRING)
            {
                return sizeof(int64_t) + sizeof(int32_t) + op.argv1_string.size();
            }
            else if (op.type == HYPERDATATYPE_MAP_INT64_INT64)
            {
                return 2 * sizeof(int64_t);
            }

            return 0;
        case OP_MAP_REMOVE:
            return 0;
        default:
            return 0;
    }
}

uint8_t*
hyperdaemon :: apply_microops(hyperdatatype type,
                              const e::slice& old_value,
                              const hyperdex::microop* ops,
                              size_t num_ops,
                              uint8_t* writeto,
                              hyperdex::network_returncode* error)
{
    switch (type)
    {
        case HYPERDATATYPE_STRING:
            return apply_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_INT64:
            return apply_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_LIST_STRING:
            return apply_list_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_LIST_INT64:
            return apply_list_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_SET_STRING:
            return apply_set_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_SET_INT64:
            return apply_set_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_STRING_STRING:
            return apply_map_string_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return apply_map_string_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return apply_map_int64_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return apply_map_int64_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_GARBAGE:
        default:
            *error = hyperdex::NET_BADMICROS;
            return NULL;
    }
}
