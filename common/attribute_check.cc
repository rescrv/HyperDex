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

// e
#include <e/endian.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/serialization.h"

using hyperdex::attribute_check;

attribute_check :: attribute_check()
    : attr()
    , value()
    , datatype(HYPERDATATYPE_GARBAGE)
    , predicate(HYPERPREDICATE_FAIL)
{
}

attribute_check :: ~attribute_check() throw ()
{
}

bool
hyperdex :: validate_attribute_check(hyperdatatype type,
                                     const attribute_check& check)
{
    datatype_info* di_attr = datatype_info::lookup(type);
    datatype_info* di_check = datatype_info::lookup(check.datatype);

    if (!di_attr || !di_check)
    {
        return false;
    }

    if (di_attr->document())
    {
        return true;
    }

    if (!di_check->validate(check.value))
    {
        return false;
    }

    switch (check.predicate)
    {
        case HYPERPREDICATE_FAIL:
            return true;
        case HYPERPREDICATE_EQUALS:
            return di_attr->datatype() == di_check->datatype();
        case HYPERPREDICATE_LESS_THAN:
        case HYPERPREDICATE_LESS_EQUAL:
        case HYPERPREDICATE_GREATER_EQUAL:
        case HYPERPREDICATE_GREATER_THAN:
            return di_attr->datatype() == di_check->datatype() &&
                   di_attr->comparable();
        case HYPERPREDICATE_REGEX:
            return di_check->datatype() == HYPERDATATYPE_STRING &&
                   di_attr->has_regex();
        case HYPERPREDICATE_CONTAINS_LESS_THAN:
        case HYPERPREDICATE_LENGTH_EQUALS:
        case HYPERPREDICATE_LENGTH_LESS_EQUAL:
        case HYPERPREDICATE_LENGTH_GREATER_EQUAL:
            return di_check->datatype() == HYPERDATATYPE_INT64 &&
                   di_attr->has_length();
        case HYPERPREDICATE_CONTAINS:
            return di_attr->has_contains() &&
                   di_attr->contains_datatype() == di_check->datatype();
        default:
            return false;
    }
}

size_t
hyperdex :: validate_attribute_checks(const schema& sc,
                                      const std::vector<attribute_check>& checks)
{
    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].attr >= sc.attrs_sz)
        {
            return i;
        }

        hyperdatatype type = sc.attrs[checks[i].attr].type;

        if (!validate_attribute_check(type, checks[i]))
        {
            return i;
        }
    }

    return checks.size();
}

bool
hyperdex :: passes_attribute_check(hyperdatatype type,
                                   const attribute_check& check,
                                   const e::slice& value)
{
    datatype_info* di_attr = datatype_info::lookup(type);
    datatype_info* di_check = datatype_info::lookup(check.datatype);

    if (!di_attr || !di_check)
    {
        return false;
    }

    if (di_attr->document())
    {
        return di_attr->document_check(check, value);
    }

    if (!di_attr->validate(value) ||
        !di_check->validate(check.value))
    {
        return false;
    }

    char buf_i[sizeof(int64_t)];
    int64_t tmp_i;

    switch (check.predicate)
    {
        case HYPERPREDICATE_FAIL:
            return false;
        case HYPERPREDICATE_EQUALS:
            return di_attr->datatype() == di_check->datatype() &&
                   (check.value == value ||
                    (di_attr->comparable() && di_attr->compare(check.value, value) == 0));
        case HYPERPREDICATE_LESS_THAN:
            return di_attr->datatype() == di_check->datatype() &&
                   di_attr->comparable() &&
                   di_attr->compare(check.value, value) > 0;
        case HYPERPREDICATE_LESS_EQUAL:
            return di_attr->datatype() == di_check->datatype() &&
                   di_attr->comparable() &&
                   di_attr->compare(check.value, value) >= 0;
        case HYPERPREDICATE_GREATER_EQUAL:
            return di_attr->datatype() == di_check->datatype() &&
                   di_attr->comparable() &&
                   di_attr->compare(check.value, value) <= 0;
        case HYPERPREDICATE_GREATER_THAN:
            return di_attr->datatype() == di_check->datatype() &&
                   di_attr->comparable() &&
                   di_attr->compare(check.value, value) < 0;
        case HYPERPREDICATE_REGEX:
            return di_check->datatype() == HYPERDATATYPE_STRING &&
                   di_attr->has_regex() &&
                   di_attr->regex(check.value, value);
        case HYPERPREDICATE_LENGTH_EQUALS:
            memset(buf_i, 0, sizeof(int64_t));
            memmove(buf_i, check.value.data(), std::min(check.value.size(), sizeof(int64_t)));
            e::unpack64le(buf_i, &tmp_i);
            return di_check->datatype() == HYPERDATATYPE_INT64 &&
                   di_attr->has_length() &&
                   static_cast<int64_t>(di_attr->length(value)) == tmp_i;
        case HYPERPREDICATE_CONTAINS_LESS_THAN:
        case HYPERPREDICATE_LENGTH_LESS_EQUAL:
            memset(buf_i, 0, sizeof(int64_t));
            memmove(buf_i, check.value.data(), std::min(check.value.size(), sizeof(int64_t)));
            e::unpack64le(buf_i, &tmp_i);
            return di_check->datatype() == HYPERDATATYPE_INT64 &&
                   di_attr->has_length() &&
                   static_cast<int64_t>(di_attr->length(value)) <= tmp_i;
        case HYPERPREDICATE_LENGTH_GREATER_EQUAL:
            memset(buf_i, 0, sizeof(int64_t));
            memmove(buf_i, check.value.data(), std::min(check.value.size(), sizeof(int64_t)));
            e::unpack64le(buf_i, &tmp_i);
            return di_check->datatype() == HYPERDATATYPE_INT64 &&
                   di_attr->has_length() &&
                   static_cast<int64_t>(di_attr->length(value)) >= tmp_i;
        case HYPERPREDICATE_CONTAINS:
            return di_attr->has_contains() &&
                   di_attr->contains_datatype() == di_check->datatype() &&
                   di_attr->contains(value, check.value);
        default:
            return false;
    }
}

size_t
hyperdex :: passes_attribute_checks(const schema& sc,
                                    const std::vector<hyperdex::attribute_check>& checks,
                                    const e::slice& key,
                                    const std::vector<e::slice>& value)
{
    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].attr >= sc.attrs_sz)
        {
            return i;
        }

        hyperdatatype type = sc.attrs[checks[i].attr].type;

        if (checks[i].attr > 0 &&
            !passes_attribute_check(type, checks[i], value[checks[i].attr - 1]))
        {
            return i;
        }
        else if (checks[i].attr == 0 &&
                 !passes_attribute_check(type, checks[i], key))
        {
            return i;
        }
    }

    return checks.size();
}

bool
hyperdex :: operator < (const attribute_check& lhs, const attribute_check& rhs)
{
    return lhs.attr < rhs.attr;
}

e::packer
hyperdex :: operator << (e::packer lhs, const attribute_check& rhs)
{
    return lhs << rhs.attr
               << rhs.value
               << rhs.datatype
               << rhs.predicate;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, attribute_check& rhs)
{
    return lhs >> rhs.attr
               >> rhs.value
               >> rhs.datatype
               >> rhs.predicate;
}

size_t
hyperdex :: pack_size(const attribute_check& rhs)
{
    return sizeof(uint16_t)
         + sizeof(uint32_t)
         + rhs.value.size()
         + pack_size(rhs.datatype)
         + pack_size(rhs.predicate);
}
