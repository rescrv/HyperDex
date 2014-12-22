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

#ifndef hyperdex_common_datatype_document_h_
#define hyperdex_common_datatype_document_h_

// HyperDex
#include "namespace.h"
#include "common/datatype_info.h"

BEGIN_HYPERDEX_NAMESPACE

class datatype_document : public datatype_info
{
    public:
        datatype_document();
        virtual ~datatype_document() throw ();

    public:
        virtual hyperdatatype datatype() const;
        virtual bool validate(const e::slice& value) const;
        virtual bool check_args(const funcall& func) const;
        virtual bool apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           e::arena* new_memory,
                           e::slice* new_value) const;

    public:
        virtual bool client_to_server(const e::slice& client,
                                      e::arena* new_memory,
                                      e::slice* server) const;
        virtual bool server_to_client(const e::slice& server,
                                      e::arena* new_memory,
                                      e::slice* client) const;

    public:
        virtual bool document() const;
        virtual bool document_check(const attribute_check& check,
                                    const e::slice& value) const;

    public:
        bool extract_value(const char* path,
                           const e::slice& data,
                           hyperdatatype* type,
                           std::vector<char>* scratch,
                           e::slice* value) const;

    private:
        void coerce_primitive_to_binary(hyperdatatype type,
                                        const e::slice& in,
                                        std::vector<char>* scratch,
                                        e::slice* value) const;
        bool coerce_binary_to_primitive(const e::slice& in,
                                        hyperdatatype* type,
                                        std::vector<char>* scratch,
                                        e::slice* value) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_datatype_document_h_
