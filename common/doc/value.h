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

#ifndef _INCLUDE_DOCUMENT_VALUE_
#define _INCLUDE_DOCUMENT_VALUE_

#include <stdint.h>
#include <string>
#include "common/doc/element_type.h"

namespace hyperdex
{
namespace doc
{

class value
{
public:
    virtual ~value() {}

    virtual element_type get_type() const = 0;

    virtual const char* get_string() const = 0;

    virtual int get_int() const = 0;
    virtual double get_float() const = 0;

    virtual const uint8_t* data() const = 0;
    virtual size_t size() const = 0;
};

value* create_value(double fp);
value* create_value(int64_t integer);
value* create_value(const char* str, size_t len);
value* create_value(bool boolean);

inline value* create_value(const std::string& str)
{
    return create_value(str.c_str(), str.size());
}

// Create a value from the binary representation of a document
value* create_document_value(const uint8_t* data, size_t size);

// Create a value from the binary representation of an array
value* create_array_value(const uint8_t* data, size_t size);

}
}

#endif


