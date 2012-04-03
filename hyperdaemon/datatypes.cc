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

// STL
#include <algorithm>

// HyperDaemon
#include "hyperdaemon/datatypes.h"

static bool
validate_list_string(const e::slice& data)
{
    const uint8_t* ptr = data.data();
    const uint8_t* const end = data.data() + data.size();

    while (ptr < end)
    {
        if (static_cast<size_t>(end - ptr) < sizeof(uint32_t))
        {
            return false;
        }

        uint32_t sz = 0;
        memmove(&sz, ptr, sizeof(uint32_t));
        sz = le32toh(sz);
        ptr += sizeof(uint32_t) + sz;
    }

    return ptr == end;
}

static bool
validate_list_int64(const e::slice& data)
{
    return (data.size() & 0x7) == 0;
}

bool
hyperdaemon :: validate_datatype(hyperdex::datatype datatype, const e::slice& data)
{
    switch (datatype)
    {
        case hyperdex::DATATYPE_STRING:
            return true;
        case hyperdex::DATATYPE_INT64:
            return data.size() <= 8;
        case hyperdex::DATATYPE_LIST_STRING:
            return validate_list_string(data);
        case hyperdex::DATATYPE_LIST_INT64:
            return validate_list_int64(data);
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
            // Allocate enough for a uint64_t, or a uint32_t with a string.  If
            // the string is empty, it will overallocate 4b, but that is OK.
            return sizeof(uint64_t) + op.argv1_string.size();
        default:
            return 0;
    }
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

static uint8_t*
apply_int64(const e::slice& old_value,
            const hyperdex::microop* ops,
            size_t num_ops,
            uint8_t* writeto,
            hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    int64_t number = 0;
    memmove(&number, old_value.data(), std::min(old_value.size(), sizeof(number)));
    number = le64toh(number);

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
            case OP_FAIL:
            default:
                *error = NET_BADMICROS;
                return NULL;
        }
    }

    number = htole64(number);
    memmove(writeto, &number, sizeof(number));
    return writeto + sizeof(number);
}

static uint8_t*
apply_list(uint8_t* (*apply_list_elem)(const hyperdex::microop* op,
                                       uint8_t* writeto,
                                       hyperdex::network_returncode* error),
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

        writeto = apply_list_elem(ops + i, writeto, error);

        if (!writeto)
        {
            return writeto;
        }
    }

    // Copy the middle of the list
    memmove(writeto, old_value.data(), old_value.size());
    writeto += old_value.size();

    // Apply the rpush ops
    for (size_t i = 0; i < num_ops; ++i)
    {
        if (ops->action != hyperdex::OP_LIST_RPUSH)
        {
            continue;
        }

        writeto = apply_list_elem(ops + i, writeto, error);

        if (!writeto)
        {
            return writeto;
        }
    }

    return writeto;
}

static uint8_t*
apply_list_string_elem(const hyperdex::microop* op,
                       uint8_t* writeto,
                       hyperdex::network_returncode* /*error*/)
{
    uint32_t sz = op->argv1_string.size();
    sz = le32toh(sz);
    memmove(writeto, &sz, sizeof(uint32_t));
    writeto += sizeof(uint32_t);
    memmove(writeto, op->argv1_string.data(), op->argv1_string.size());
    return writeto + op->argv1_string.size();
}

static uint8_t*
apply_list_string(const e::slice& old_value,
                  const hyperdex::microop* ops,
                  size_t num_ops,
                  uint8_t* writeto,
                  hyperdex::network_returncode* error)
{
    return apply_list(apply_list_string_elem, old_value, ops, num_ops, writeto, error);
}

static uint8_t*
apply_list_int64_elem(const hyperdex::microop* op,
                      uint8_t* writeto,
                      hyperdex::network_returncode* /*error*/)
{
    int64_t number = op->argv1_int64;
    number = le64toh(number);
    memmove(writeto, &number, sizeof(int64_t));
    return writeto + sizeof(int64_t);
}

static uint8_t*
apply_list_int64(const e::slice& old_value,
                 const hyperdex::microop* ops,
                 size_t num_ops,
                 uint8_t* writeto,
                 hyperdex::network_returncode* error)
{
    return apply_list(apply_list_int64_elem, old_value, ops, num_ops, writeto, error);
}

uint8_t*
hyperdaemon :: apply_microops(hyperdex::datatype type,
                              const e::slice& old_value,
                              const hyperdex::microop* ops,
                              size_t num_ops,
                              uint8_t* writeto,
                              hyperdex::network_returncode* error)
{
    using namespace hyperdex;

    switch (type)
    {
        case DATATYPE_STRING:
            return apply_string(old_value, ops, num_ops, writeto, error);
        case DATATYPE_INT64:
            return apply_int64(old_value, ops, num_ops, writeto, error);
        case DATATYPE_LIST_STRING:
            return apply_list_string(old_value, ops, num_ops, writeto, error);
        case DATATYPE_LIST_INT64:
            return apply_list_int64(old_value, ops, num_ops, writeto, error);
        default:
            *error = NET_BADMICROS;
            return NULL;
    }
}
