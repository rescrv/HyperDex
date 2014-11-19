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
        bson_t* add_or_replace_value(const bson_t* old_document, const json_path& path, const bson_value_t *value) const;
        void add_or_replace_value_recurse(const json_path& path, const bson_value_t *value,
                                    bson_t* parent, bson_iter_t* iter) const;

        bool can_create_element(const bson_t &doc, json_path path) const;
        bson_type_t get_element_type(const bson_t &document, const json_path& path) const;

        bool does_entry_exist(const bson_t &document, const json_path& path) const;

        bson_t* unset_value(const bson_t* old_document, const json_path& path) const;
        void unset_value_recurse(const json_path& path, bson_t* parent, bson_iter_t* iter) const;

        bson_t* rename_value(const bson_t* old_document, const json_path& path, const std::string& new_name) const;
        void rename_value_recurse(const json_path& path, const std::string& new_name, bson_t* parent, bson_iter_t* iter) const;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_datatype_document_h_
