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

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/apply.h"
#include "datatypes/validate.h"

using hyperdex::attribute_check;
using hyperdex::funcall;

bool
passes_attribute_check(hyperdatatype type,
                       const attribute_check& check,
                       const e::slice& value,
                       microerror* error)
{
    switch (check.pred)
    {
        case hyperdex::PRED_EQUALS:
            *error = MICROERR_CMPFAIL;
            return validate_as_type(check.value, check.datatype) &&
                   type == check.datatype &&
                   check.value == value;
        default:
            return false;
    }
}

static uint8_t*
apply_funcalls(hyperdatatype type,
               const e::slice& old_value,
               const funcall* funcs, size_t num_funcs,
               uint8_t* writeto, microerror* error)
{
    switch (type)
    {
        case HYPERDATATYPE_STRING:
            return apply_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_INT64:
            return apply_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_FLOAT:
            return apply_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_LIST_STRING:
            return apply_list_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_LIST_INT64:
            return apply_list_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_LIST_FLOAT:
            return apply_list_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_SET_STRING:
            return apply_set_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_SET_INT64:
            return apply_set_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_SET_FLOAT:
            return apply_set_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_STRING_STRING:
            return apply_map_string_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return apply_map_string_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return apply_map_string_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return apply_map_int64_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return apply_map_int64_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return apply_map_int64_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return apply_map_float_string(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return apply_map_float_int64(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return apply_map_float_float(old_value, funcs, num_funcs, writeto, error);
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
        default:
            *error = MICROERROR;
            return NULL;
    }
}

size_t
perform_checks_and_apply_funcs(const hyperdex::schema* sc,
                               const std::vector<attribute_check>& checks,
                               const std::vector<funcall>& funcs,
                               const e::slice& old_key,
                               const std::vector<e::slice>& old_value,
                               std::tr1::shared_ptr<e::buffer>* new_backing,
                               e::slice* new_key,
                               std::vector<e::slice>* new_value,
                               microerror* error)
{
    // Check the checks
    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].attr >= sc->attrs_sz)
        {
            *error = MICROERR_UNKNOWNATTR;
            return i;
        }

        if (checks[i].attr > 0 &&
            !passes_attribute_check(sc->attrs[checks[i].attr].type, checks[i],
                               old_value[checks[i].attr - 1], error))
        {
            return i;
        }
        else if (checks[i].attr == 0 &&
                 !passes_attribute_check(sc->attrs[0].type, checks[i], old_key, error))
        {
            return i;
        }
    }

    // There's a fixed size for the key.
    size_t sz = old_key.size();

    // We'll start with the size of the old value.
    for (size_t i = 0; i < old_value.size(); ++i)
    {
        sz += old_value[i].size();
    }

    // Now we'll add the size of each funcall
    for (size_t i = 0; i < funcs.size(); ++i)
    {
        sz += 2 * sizeof(uint32_t)
            + funcs[i].arg1.size()
            + funcs[i].arg2.size();
    }

    // Allocate the new buffer
    new_backing->reset(e::buffer::create(sz));
    new_value->resize(old_value.size());

    // Write out the object into new_backing
    uint8_t* write_to = (*new_backing)->data();

    // Copy the key (which never is touched by funcalls)
    *new_key = e::slice(write_to, old_key.size());
    memmove(write_to, old_key.data(), old_key.size());
    write_to += old_key.size();

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
            if (end->attr == 0)
            {
                *error = MICROERR_DONTUSEKEY;
                return checks.size() + (op - &funcs.front());
            }

            if (end->attr >= sc->attrs_sz)
            {
                *error = MICROERR_UNKNOWNATTR;
                return checks.size() + (op - &funcs.front());
            }

            if (end->attr < next_to_copy)
            {
                *error = MICROERR_UNSORTEDOPS;
                return checks.size() + (op - &funcs.front());
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
        uint8_t* new_write_to = apply_funcalls(sc->attrs[op->attr].type,
                                               old_value[op->attr - 1],
                                               op, end - op,
                                               write_to, error);

        if (!new_write_to)
        {
            return checks.size() + (op - &funcs.front());
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
    while (next_to_copy < sc->attrs_sz)
    {
        assert(next_to_copy > 0);
        size_t idx = next_to_copy - 1;
        memmove(write_to, old_value[idx].data(), old_value[idx].size());
        (*new_value)[idx] = e::slice(write_to, old_value[idx].size());
        write_to += old_value[idx].size();
        ++next_to_copy;
    }

    return checks.size() + funcs.size();
}
