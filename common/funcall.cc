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
#include "common/datatypes.h"
#include "common/funcall.h"

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
        if (!validate_func(sc, funcs[i]))
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
                        std::auto_ptr<e::buffer>* backing,
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

    // Allocate the new buffer
    backing->reset(e::buffer::create(sz));
    (*backing)->resize(sz);
    new_value->resize(old_value.size());

    // Write out the object into new_backing
    uint8_t* write_to = (*backing)->data();

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
            memmove(write_to, old_value[idx].data(), old_value[idx].size());
            (*new_value)[idx] = e::slice(write_to, old_value[idx].size());
            write_to += old_value[idx].size();
            ++next_to_copy;
        }

        // - "op" now points to the first unapplied funcall
        // - "end" points to one funcall past the last op we want to apply
        // - we've copied all attributes so far, even those not mentioned by
        //   funcs.

        // This call may modify [op, end) funcs.
        datatype_info* di = datatype_info::lookup(sc.attrs[op->attr].type);
        uint8_t* new_write_to = di->apply(old_value[op->attr - 1],
                                          op, end - op, write_to);

        if (!new_write_to)
        {
            return (op - &funcs.front());
        }

        (*new_value)[next_to_copy - 1] = e::slice(write_to, new_write_to - write_to);
        write_to = new_write_to;

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
        memmove(write_to, old_value[idx].data(), old_value[idx].size());
        (*new_value)[idx] = e::slice(write_to, old_value[idx].size());
        write_to += old_value[idx].size();
        ++next_to_copy;
    }

    return funcs.size();
}

bool
hyperdex :: operator < (const funcall& lhs, const funcall& rhs)
{
    return lhs.attr < rhs.attr;
}
