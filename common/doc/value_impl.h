// Copyright (c) 2014, Cornell University
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

#ifndef _INCLUDE_DOCUMENT_VALUE_IMPL_
#define _INCLUDE_DOCUMENT_VALUE_IMPL_

#include <bson.h>
#include "common/doc/value.h"

namespace hyperdex
{
namespace doc
{

class value_impl : public value
{
public:
    ~value_impl() {}

    virtual const char* get_string() const;

    virtual int get_int() const;
    virtual double get_float() const;

    virtual const uint8_t* data() const;
    virtual size_t size() const;

    virtual bson_value_t to_bson() const = 0;
};


class value_float : public value_impl
{
public:
    value_float(double value_) : value(value_) {}
    bson_value_t to_bson() const;

    double get_float() const
    {
        return value;
    }

    element_type get_type() const
    {
        return element_type_float;
    }

private:
    const double value;
};

class value_int : public value_impl
{
public:
    value_int(int64_t value_) : value(value_) {}
    bson_value_t to_bson() const;

    int get_int() const
    {
        return value;
    }

    element_type get_type() const
    {
        return element_type_integer;
    }

private:
    const int64_t value;
};

class value_string : public value_impl
{
public:
    value_string(const char* value_, const size_t len_) : value(value_), len(len_) {}
    bson_value_t to_bson() const;

    const char* get_string() const
    {
        return value;
    }

    element_type get_type() const
    {
        return element_type_string;
    }

    size_t size() const
    {
        return len;
    }

private:
    const char* value;
    const size_t len;
};

class value_bool : public value_impl
{
public:
    value_bool(bool value_) : value(value_) {}
    bson_value_t to_bson() const;

    element_type get_type() const
    {
        return element_type_bool;
    }

private:
    const bool value;
};

class value_bson_document : public value_impl
{
public:
    value_bson_document(const uint8_t* data_, size_t size_);
    bson_value_t to_bson() const;

    element_type get_type() const
    {
        return element_type_document;
    }

    const uint8_t* data() const;
    size_t size() const;

private:
    const uint8_t* mdata;
    const size_t msize;
};

class value_bson_array : public value_impl
{
public:
    value_bson_array(const uint8_t* data_, size_t size_);
    bson_value_t to_bson() const;

    element_type get_type() const
    {
        return element_type_array;
    }

    const uint8_t* data() const;
    size_t size() const;

private:
    const uint8_t* mdata;
    const size_t msize;
};

}
}

#endif
