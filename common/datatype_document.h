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
#include "common/json_path.h"

class json_object;

#include <bson.h>

BEGIN_HYPERDEX_NAMESPACE

class datatype_document : public datatype_info
{
    public:
        datatype_document();
        virtual ~datatype_document() throw ();

    public:
        virtual hyperdatatype datatype() const;
        virtual bool validate(const e::slice& value) const;
        virtual bool validate_old_values(const std::vector<e::slice>& old_values, const funcall& func) const;
        virtual bool check_args(const funcall& func) const;
        virtual uint8_t* apply(const e::slice& old_value,
                               const funcall* funcs, size_t funcs_sz,
                               uint8_t* writeto);

    public:
        virtual bool document() const;
        virtual bool document_check(const attribute_check& check,
                                    const e::slice& value);

    public:
        // Retrieve value in a json document by traversing it
        // Will allocate a buffer for the data and a slice referencing it
        bool extract_value(const std::string& path,
                        const e::slice& document, // the whole document
                        hyperdatatype* type, // OUT: the datatype of the result
                        std::vector<char>* scratch, // OUT: the resulting content/value
                        e::slice* value); // OUT: slice to easier access the content of the scratch

    private:
        // Get the last element of a path (and its name and parent)
        void get_end(const json_object* root, const json_path& path,
                                json_object*& parent, json_object*& obj, std::string& obj_name) const;

        // Traverse a path to the last node
        // Returns NULL if the node doesn't exist
        json_object* traverse_path(const json_object* root, const json_path& path) const;

        // Go down the path as far as possible
        json_object* get_last_elem_in_path(const json_object* parent, const json_path& path, json_path& child_path) const;

        // Create a new document where one string entry is replaced by a new value
        bson_t* add_or_replace_string(const bson_t* old_document, const json_path& path, const std::string& new_val) const;
        void replace_string_recurse(const json_path& path, const std::string& new_value,
                                    bson_t* parent, bson_iter_t* iter) const;

        bson_t* add_or_replace_int64(const bson_t* old_document, const json_path& path, int64_t new_val) const;
        void replace_int64_recurse(const json_path& path, const int64_t new_value,
                                    bson_t* parent, bson_iter_t* iter) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_datatype_document_h_
