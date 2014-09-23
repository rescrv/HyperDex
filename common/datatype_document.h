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

class json_object;

BEGIN_HYPERDEX_NAMESPACE

class datatype_document : public datatype_info
{
    public:
        datatype_document();
        virtual ~datatype_document() throw ();

    public:
        virtual hyperdatatype datatype() const;
        virtual bool validate(const e::slice& value) const;
        virtual bool validate_old_values(const key_change& kc, const std::vector<e::slice>& old_values) const;
        virtual bool check_args(const funcall& func) const;
        virtual uint8_t* apply(const e::slice& old_value,
                               const funcall* funcs, size_t funcs_sz,
                               uint8_t* writeto);

    public:
        virtual bool document();
        virtual bool document_check(const attribute_check& check,
                                    const e::slice& value);

    public:
        // Parse for a specific keyname in a document
        bool parse_path(const char* path, // the path from root to the subtree/leaf
                        const char* const end,  // ??
                        const e::slice& document, // the whole document
                        hyperdatatype hint, // possible datatpe of the result
                        hyperdatatype* type, // OUT: the datatype of the result
                        std::vector<char>* scratch, // OUT: ??
                        e::slice* value); // OUT: the resulting content/value

    private:
        // Convert raw data into a json object
        json_object* to_json(const e::slice& slice) const;

        void atomic_add(const char* key, json_object* parent, json_object* data,
                        const std::string& path, const int64_t addval) const;

        // Traverse a path to the last node
        // Returns NULL if the node doesn't exist
        json_object* traverse_path(const json_object* root, const std::string& path) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_datatype_document_h_
