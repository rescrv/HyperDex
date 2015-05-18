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
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/serialization.h"

using hyperdex::funcall;

funcall :: funcall()
    : attr(UINT16_MAX)
    , name(FUNC_FAIL)
    , arg1()
    , arg1_datatype()
    , arg2()
    , arg2_datatype()
{
}

funcall :: ~funcall() throw ()
{
}

bool
hyperdex :: validate_func(const schema& sc, const funcall& func)
{
    if (func.attr >= sc.attrs_sz)
    {
        return false;
    }

    datatype_info* di = datatype_info::lookup(sc.attrs[func.attr].type);

    if (!di || !di->check_args(func))
    {
        return false;
    }

    return true;
}

size_t
hyperdex :: validate_funcs(const schema& sc,
                           const std::vector<funcall>& funcs)
{
    for (size_t i = 0; i < funcs.size(); ++i)
    {
        if ((i > 0 && funcs[i - 1].attr > funcs[i].attr) ||
            !validate_func(sc, funcs[i]))
        {
            return i;
        }
    }

    return funcs.size();
}

size_t
hyperdex :: apply_funcs(const schema& sc,
                        const std::vector<funcall>& funcs,
                        const e::slice& key,
                        const std::vector<e::slice>& old_value,
                        e::arena* new_memory,
                        std::vector<e::slice>* new_value)
{
    // Figure out the size of the new buffer
    size_t sz = key.size();

    // size of the old value.
    for (size_t i = 0; i < old_value.size(); ++i)
    {
        sz += old_value[i].size() + sizeof(uint64_t);
    }

    // size of the funcalls.
    for (size_t i = 0; i < funcs.size(); ++i)
    {
        sz += 2 * sizeof(uint32_t)
            + funcs[i].arg1.size()
            + funcs[i].arg2.size() + sizeof(uint64_t);
    }

    new_memory->reserve(sz);

    // Apply the funcalls to each value
    const funcall* op = funcs.empty() ? NULL : &funcs.front();
    const funcall* const stop = op + funcs.size();
    // The next attribute to copy, indexed based on the total number of
    // dimensions.  It starts at 1, because the key is 0, and 1 is the first
    // secondary attribute.
    uint16_t next_to_copy = 1;

    while (op < stop)
    {
        const funcall* end = op;

        // Advance until [op, end) is all a continuous range of funcs that all
        // apply to the same attribute value.
        while (end < stop && op->attr == end->attr)
        {
            if (end->attr == 0 ||
                end->attr >= sc.attrs_sz ||
                end->attr < next_to_copy)
            {
                return (op - &funcs.front());
            }

            ++end;
        }

        // Copy the attributes that are unaffected by funcs.
        while (next_to_copy < op->attr)
        {
            assert(next_to_copy > 0);
            size_t idx = next_to_copy - 1;
            unsigned char* tmp = NULL;
            new_memory->allocate(old_value[idx].size(), &tmp);
            memmove(tmp, old_value[idx].data(), old_value[idx].size());
            (*new_value)[idx] = e::slice(tmp, old_value[idx].size());
            ++next_to_copy;
        }

        // - "op" now points to the first unapplied funcall
        // - "end" points to one funcall past the last op we want to apply
        // - we've copied all attributes so far, even those not mentioned by
        //   funcs.

        // This call may modify [op, end) funcs.
        datatype_info* di = datatype_info::lookup(sc.attrs[op->attr].type);

        if (!di->apply(old_value[op->attr - 1], op, end - op, new_memory, &(*new_value)[next_to_copy - 1]))
        {
            return (op - &funcs.front());
        }

        // Why ++ and assert rather than straight assign?  This will help us to
        // catch any problems in the interaction between funcs and
        // attributes which we just copy.
        assert(next_to_copy == op->attr);
        ++next_to_copy;

        op = end;
    }

    // Copy the attributes that are unaffected by funcs.
    while (next_to_copy < sc.attrs_sz)
    {
        assert(next_to_copy > 0);
        size_t idx = next_to_copy - 1;
        unsigned char* tmp = NULL;
        new_memory->allocate(old_value[idx].size(), &tmp);
        memmove(tmp, old_value[idx].data(), old_value[idx].size());
        (*new_value)[idx] = e::slice(tmp, old_value[idx].size());
        ++next_to_copy;
    }

    return funcs.size();
}

bool
hyperdex :: operator < (const funcall& lhs, const funcall& rhs)
{
    return lhs.attr < rhs.attr;
}

e::packer
hyperdex :: operator << (e::packer lhs, const funcall_t& rhs)
{
    uint8_t name = static_cast<uint8_t>(rhs);
    return lhs << name;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, funcall_t& rhs)
{
    uint8_t name;
    lhs = lhs >> name;
    rhs = static_cast<funcall_t>(name);
    return lhs;
}

size_t
hyperdex :: pack_size(const funcall_t&)
{
    return sizeof(uint8_t);
}

e::packer
hyperdex :: operator << (e::packer lhs, const funcall& rhs)
{
    return lhs << rhs.attr << rhs.name
               << rhs.arg1 << rhs.arg1_datatype
               << rhs.arg2 << rhs.arg2_datatype;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, funcall& rhs)
{
    return lhs >> rhs.attr >> rhs.name
               >> rhs.arg1 >> rhs.arg1_datatype
               >> rhs.arg2 >> rhs.arg2_datatype;
}

size_t
hyperdex :: pack_size(const funcall& m)
{
    return sizeof(uint16_t) + pack_size(m.name)
         + pack_size(m.arg1) + pack_size(m.arg1_datatype)
         + pack_size(m.arg2) + pack_size(m.arg2_datatype);
}
