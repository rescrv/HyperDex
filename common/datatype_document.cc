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

#define __STDC_LIMIT_MACROS

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// HyperDex
#include "common/datatype_document.h"
#include "common/datatype_string.h"
#include "common/datatype_int64.h"
#include "common/key_change.h"
#include "common/doc/document.h"

// json-c
#if HAVE_JSON_H
#include <json/json.h>
#elif HAVE_JSON_C_H
#include <json-c/json.h>
#else
#error no suitable json.h found
#endif

// e
#include <e/endian.h>
#include <e/guard.h>
#include <e/safe_math.h>

#include <ctype.h>
#include <sstream>
#include <stdexcept>

using hyperdex::datatype_info;
using hyperdex::datatype_document;
using hyperdex::doc::create_document;
using hyperdex::doc::document;
using hyperdex::doc::path;
using hyperdex::doc::value;
using namespace hyperdex::doc;

datatype_document :: datatype_document()
{
}

datatype_document :: ~datatype_document() throw ()
{
}

hyperdatatype
datatype_document :: datatype() const
{
    return HYPERDATATYPE_DOCUMENT;
}

bool
datatype_document :: validate(const e::slice& value) const
{
    if(value.size() == 0)
    {
        // default value is empty
        // not part of the BSON standard but this is how HyperDex behaves
        return true;
    }

    std::auto_ptr<hyperdex::doc::document> doc(create_document(value.data(), value.size()));
    return doc->is_valid();
}

bool
datatype_document :: validate_old_values(const std::vector<e::slice>& old_values, const funcall& func) const
{
    if(func.name == FUNC_SET)
    {
        // Doesn matter. Old value will be overwritten anyways.
        return true;
    }

    std::auto_ptr<hyperdex::doc::document> doc( create_document(old_values[0].data(), old_values[0].size()) );
    path p = func.arg2.c_str();

    if(!doc->is_valid())
    {
        return false;
    }

    // we only need to check old values for atomic operations
    switch(func.name)
    {
    case FUNC_DOC_SET:
    {
        return !doc->does_entry_exist(p) && doc->can_create_element(p);
    }
    case FUNC_DOC_UNSET:
    {
        return doc->does_entry_exist(p);
    }
    case FUNC_DOC_RENAME:
    {
        std::string new_name(func.arg1.c_str());

        path parent_path;
        std::string obj_name;
        p.split_reverse(parent_path, obj_name);
        parent_path.append(new_name);

        return doc->does_entry_exist(p) && !doc->does_entry_exist(parent_path);
    }
    case FUNC_LIST_LPUSH:
    case FUNC_LIST_RPUSH:
    {
        return doc->can_create_element(p) || doc->get_element_type(p) == element_type_array;
    }
    case FUNC_NUM_ADD:
    case FUNC_NUM_AND:
    case FUNC_NUM_MAX:
    case FUNC_NUM_DIV:
    case FUNC_NUM_MIN:
    case FUNC_NUM_MOD:
    case FUNC_NUM_MUL:
    case FUNC_NUM_SUB:
    case FUNC_NUM_XOR:
    case FUNC_NUM_OR:
    {
        element_type type = doc->get_element_type(p);

        if(type == element_type_integer || type == element_type_float)
        {
            return true;
        }

        return doc->can_create_element(p);
        break;
    }
    case FUNC_STRING_APPEND:
    case FUNC_STRING_PREPEND:
    {
        return doc->can_create_element(p) || doc->get_element_type(p) == element_type_string;
        break;
    }
    case FUNC_FAIL:
    case FUNC_SET:
    case FUNC_SET_ADD:
    case FUNC_SET_REMOVE:
    case FUNC_SET_INTERSECT:
    case FUNC_SET_UNION:
    case FUNC_MAP_ADD:
    case FUNC_MAP_REMOVE:
    default:
        break;
    }

    // No check failed
    return true;
}

bool
datatype_document :: check_args(const funcall& func) const
{
    switch(func.name)
    {
    case FUNC_SET:
    {
        // set (or replace with) a new document
        return func.arg1_datatype == HYPERDATATYPE_DOCUMENT && validate(func.arg1);
    }
    case FUNC_NUM_ADD:
    case FUNC_NUM_SUB:
    case FUNC_NUM_MUL:
    case FUNC_NUM_DIV:
    case FUNC_NUM_XOR:
    case FUNC_NUM_AND:
    case FUNC_NUM_OR:
    case FUNC_NUM_MOD:
    case FUNC_NUM_MIN:
    case FUNC_NUM_MAX:
    {
        if(is_binary_operation(func.name) && func.arg1_datatype == HYPERDATATYPE_FLOAT)
        {
            // not defined for float
            return false;
        }

        // they second argument is a path to the field we want to manipulate
        // (the path is represented as a string)
        return is_type_numeric(func.arg1_datatype) && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_STRING_APPEND:
    case FUNC_STRING_PREPEND:
    {
        // they second argument is a path to the field we want to manipulate
        // (the path is represented as a string)
        return func.arg1_datatype == HYPERDATATYPE_STRING && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_LIST_LPUSH:
    case FUNC_LIST_RPUSH:
    case FUNC_DOC_SET:
    {
        if (!func.arg2_datatype == HYPERDATATYPE_STRING)
        {
            // not a valid path
            return false;
        }

        if(is_type_primitive(func.arg1_datatype))
        {
            return true;
        }
        else if(func.arg1_datatype == HYPERDATATYPE_DOCUMENT)
        {
            return validate(func.arg1);
        }
        else
        {
            // invalid argument
            return false;
        }
    }
    case FUNC_DOC_UNSET:
    {
        // Key should be a path and the value will not be evaluated (for now)
        return func.arg1_datatype == HYPERDATATYPE_INT64 && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_DOC_RENAME:
    {
        if(!func.arg1_datatype == HYPERDATATYPE_STRING && func.arg2_datatype == HYPERDATATYPE_STRING)
        {
            return false;
        }

        // New name must not be empty or contain a '.'
        std::string new_path = func.arg1.c_str();
        return !new_path.empty() && new_path.find('.') == std::string::npos;
    }
    case FUNC_FAIL:
    case FUNC_SET_ADD:
    case FUNC_SET_INTERSECT:
    case FUNC_SET_REMOVE:
    case FUNC_SET_UNION:
    case FUNC_MAP_ADD:
    case FUNC_MAP_REMOVE:
    default:
    {
        // Unsupported operation
        return false;
    }
    }
}

value*
datatype_document :: encode_value(const hyperdatatype type, const e::slice& data) const
{
    if(type == HYPERDATATYPE_STRING)
    {
        return create_value(data.c_str(), data.size());
    }
    else if(type == HYPERDATATYPE_FLOAT)
    {
        double arg = reinterpret_cast<const double*>(data.data())[0];
        return create_value(arg);
    }
    else if(type == HYPERDATATYPE_INT64)
    {
        int64_t arg = reinterpret_cast<const int64_t*>(data.data())[0];
        return create_value(arg);
    }
    else if(type == HYPERDATATYPE_DOCUMENT)
    {
        return create_document_value(data.data(), data.size());
    }
    else
    {
        throw std::runtime_error("Cannot encode value: Unknown datatype");
    }
}

uint8_t*
datatype_document :: apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           uint8_t* writeto)
{
    // Prevent copying on SET
    if(funcs_sz == 1 && funcs[0].name == FUNC_SET)
    {
        size_t len = funcs[0].arg1.size();
        memmove(writeto, funcs[0].arg1.data(), len);
        return writeto + len;
    }

    e::slice new_value = old_value;

    // To support multiple updates on the same document
    // we reuse the same object
    hyperdex::doc::document *doc = NULL;

    for (size_t pos = 0; pos < funcs_sz; ++pos)
    {
        const funcall& func = *(funcs + pos);

        switch(func.name)
        {
        case FUNC_SET:
        {
            // this is not supported
            // there should be only a single func call when set happens
            abort();
            break;
        }
        case FUNC_DOC_SET:
        {
            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            path p = func.arg2.c_str();

            value* value = encode_value(func.arg1_datatype, func.arg1);
            hyperdex::doc::document *new_doc = doc->add_or_replace_value(p, *value);

            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_DOC_UNSET:
        {
            doc = doc ? doc : create_document(old_value.data(), old_value.size());
            std::string p = func.arg2.c_str();
            hyperdex::doc::document* new_doc = doc->unset_value(p);

            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_DOC_RENAME:
        {
            std::string new_name(func.arg1.cdata(), func.arg1.size());
            path p = func.arg2.c_str();

            doc = doc ? doc : create_document(old_value.data(), old_value.size());
            hyperdex::doc::document* new_doc = doc->rename_value(p, new_name);

            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_STRING_PREPEND:
        case FUNC_STRING_APPEND:
        {
            std::string arg(func.arg1.cdata(), func.arg1.size());
            path p = func.arg2.c_str();

            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            std::string str = "";
            std::auto_ptr<value> val (doc->extract_value(p));

            if(val.get() != NULL)
            {
                assert(val->get_type() == element_type_string);
                str = val->get_string();
            }

            if(func.name == FUNC_STRING_APPEND)
            {
                str = str + arg;
            }
            else
            {
                str = arg + str;
            }

            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            std::auto_ptr<value> new_val (create_value(str));
            hyperdex::doc::document* new_doc = doc->add_or_replace_value(p, *new_val);

            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_NUM_ADD:
        case FUNC_NUM_SUB:
        case FUNC_NUM_DIV:
        case FUNC_NUM_MUL:
        case FUNC_NUM_XOR:
        case FUNC_NUM_OR:
        case FUNC_NUM_AND:
        case FUNC_NUM_MOD:
        case FUNC_NUM_MAX:
        case FUNC_NUM_MIN:
        {
            bool arg_is_int = false;
            int64_t int_arg = 0;
            double float_arg = 0.0;

            if(func.arg1_datatype == HYPERDATATYPE_INT64)
            {
                arg_is_int = true;
                int_arg = reinterpret_cast<const int64_t*>(func.arg1.data())[0];
            }
            else
            {
                arg_is_int = false;
                float_arg = reinterpret_cast<const double*>(func.arg1.data())[0];
            }

            path p = func.arg2.c_str();
            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            int64_t int_field = 0;
            double float_field = 0.0;

            bool field_is_int = true;
            bool field_exists = false;

            std::auto_ptr<value> val (doc->extract_value(p));

            if(val.get() != NULL)
            {
                field_exists = true;

                if(val->get_type() == element_type_integer)
                {
                    field_is_int = true;
                    int_field = val->get_int();
                }
                else
                {
                    field_is_int = false;
                    float_field = val->get_float();
                }
            }


            // if one of the two is a float we have to convert
            // and also can't simply overwrite the value
            bool float_op = false;
            if(arg_is_int && !field_is_int)
            {
                float_op = true;
                field_exists = false;
                float_arg = int_arg;
            }
            else if(!arg_is_int && field_is_int)
            {
                float_op = true;
                field_exists = false;
                float_field = int_field;
            }
            else if(!arg_is_int && !field_is_int)
            {
                float_op = true;
            }

            bool success = false;

            if(float_op)
            {
                // no overflow checks here...
                success = true;

                if(func.name == FUNC_NUM_ADD)
                {
                    float_field = float_field + float_arg;
                }
                else if(func.name == FUNC_NUM_SUB)
                {
                    float_field = float_field - float_arg;
                }
                else if(func.name == FUNC_NUM_DIV)
                {
                    float_field = float_field / float_arg;
                }
                else if(func.name == FUNC_NUM_MUL)
                {
                    float_field = float_field * float_arg;
                }
                else if(func.name == FUNC_NUM_MAX)
                {
                    float_field = std::max(float_field, float_arg);
                }
                else if(func.name == FUNC_NUM_MIN)
                {
                    float_field = std::min(float_field, float_arg);
                }
                else
                {
                    success = false;
                }
            }
            else
            {
                if(func.name == FUNC_NUM_ADD)
                {
                    success = e::safe_add(int_field, int_arg, &int_field);
                }
                else if(func.name == FUNC_NUM_SUB)
                {
                    success = e::safe_sub(int_field, int_arg, &int_field);
                }
                else if(func.name == FUNC_NUM_DIV)
                {
                    success = e::safe_div(int_field, int_arg, &int_field);
                }
                else if(func.name == FUNC_NUM_MUL)
                {
                    success = e::safe_mul(int_field, int_arg, &int_field);
                }
                else if(func.name == FUNC_NUM_MOD)
                {
                    int_field = int_field % int_arg;
                    success = true;
                }
                else if(func.name == FUNC_NUM_XOR)
                {
                    int_field = int_field ^ int_arg;
                    success = true;
                }
                else if(func.name == FUNC_NUM_AND)
                {
                    int_field = int_field & int_arg;
                    success = true;
                }
                else if(func.name == FUNC_NUM_OR)
                {
                    int_field = int_field | int_arg;
                    success = true;
                }
                else if(func.name == FUNC_NUM_MAX)
                {
                    int_field = std::max(int_field, int_arg);
                    success = true;
                }
                else if(func.name == FUNC_NUM_MIN)
                {
                    int_field = std::min(int_field, int_arg);
                    success = true;
                }
            }

            if(success)
            {
               if(field_exists)
                {
                    if(float_op)
                    {
                        doc->overwrite_float(p, float_field);
                    }
                    else
                    {
                        doc->overwrite_integer(p, int_field);
                    }
                }
                else
                {
                    value *val = NULL;

                    if(float_op)
                    {
                        val = create_value(float_field);
                    }
                    else
                    {
                        val = create_value(int_field);
                    }

                    hyperdex::doc::document* new_doc = doc->add_or_replace_value(p, *val);

                    delete doc;
                    delete val;
                    doc = new_doc;
                }
            }
            else
            {
                delete doc;
                return NULL;
            }
            break;
        }
        case FUNC_LIST_LPUSH:
        {
            std::string arg(func.arg1.cdata(), func.arg1.size());
            path p = func.arg2.c_str();
            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            // Put new value at the front
            value *val = encode_value(func.arg1_datatype, func.arg1);
            hyperdex::doc::document *new_doc = doc->array_prepend(p, *val);

            delete val;
            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_LIST_RPUSH:
        {
            std::string arg(func.arg1.cdata(), func.arg1.size());
            path p = func.arg2.c_str();
            doc = doc ? doc : create_document(old_value.data(), old_value.size());

            value *val = encode_value(func.arg1_datatype, func.arg1);
            hyperdex::doc::document *new_doc = doc->array_append(p, *val);

            delete val;
            delete doc;
            doc = new_doc;
            break;
        }
        case FUNC_FAIL:
        case FUNC_SET_ADD:
        case FUNC_SET_INTERSECT:
        case FUNC_SET_REMOVE:
        case FUNC_SET_UNION:
        case FUNC_MAP_ADD:
        case FUNC_MAP_REMOVE:
        default:
            abort();
        }
    }

    if(doc)
    {
        size_t len = doc->size();
        const uint8_t* data = doc->data();

        memmove(writeto, data, len);
        delete doc;

        return writeto + len;
    }
    else
    {
        abort();
        return NULL;
    }
}

bool
datatype_document :: document() const
{
    return true;
}

bool
datatype_document :: document_check(const attribute_check& check,
                                    const e::slice& doc)
{
    // a check that compares the whole document with another document
    if (check.datatype == HYPERDATATYPE_DOCUMENT)
    {
        return check.predicate == HYPERPREDICATE_EQUALS &&
               check.value == doc;
    }
    // we compare/evaluate one value of the document
    else
    {
        // Search for a \0-character and cut the string of after that point
        const char* cstr = reinterpret_cast<const char*>(check.value.data());
        size_t path_sz = strnlen(cstr, check.value.size());

        // If we don't find a null character we terminate
        // because there is no value following the path information
        // so check.value should be in the following form: <path>\0<value>
        if(path_sz >= check.value.size())
        {
            return false;
        }

        std::string path(std::string(cstr, path_sz));

        hyperdatatype type;
        std::vector<char> scratch;
        e::slice value;

        if (!extract_value(path, doc, &type, &scratch, &value))
        {
            return false;
        }

        attribute_check new_check;
        new_check.attr      = check.attr;
        new_check.value     = check.value;
        new_check.datatype  = check.datatype;
        new_check.predicate = check.predicate;
        new_check.value.advance(path.size() + 1);
        return passes_attribute_check(type, new_check, value);
    }
}

bool
datatype_document :: extract_value(const path& p,
                                const e::slice& data,
                                hyperdatatype* type,
                                std::vector<char>* scratch,
                                e::slice* value)
{
    hyperdex::doc::document* doc = create_document(data.data(), data.size());
    hyperdex::doc::value *val = doc->extract_value(p);

    element_type etype = val->get_type();

    if (etype == element_type_float)
    {
        const size_t number_sz = sizeof(double);

        if (scratch->size() < number_sz)
        {
            scratch->resize(number_sz);
        }

        double d = val->get_float();
        e::packdoublele(d, &(*scratch)[0]);
        *type = HYPERDATATYPE_FLOAT;
        *value = e::slice(&(*scratch)[0], sizeof(double));

        return true;
    }
    else if(etype == element_type_integer)
    {
        const size_t number_sz =  sizeof(int64_t);

        if (scratch->size() < number_sz)
        {
            scratch->resize(number_sz);
        }

        int64_t i = val->get_int();
        e::pack64le(i, &(*scratch)[0]);
        *type = HYPERDATATYPE_INT64;
        *value = e::slice(&(*scratch)[0], sizeof(int64_t));
        return true;
    }
    else if (etype == element_type_string)
    {
        const char* str = val->get_string();
        size_t str_sz = strlen(str);

        if (scratch->size() < str_sz)
        {
            scratch->resize(str_sz);
        }

        memmove(&scratch->front(), str, str_sz);
        *type = HYPERDATATYPE_STRING;
        *value = e::slice(&(*scratch)[0], str_sz);
        return true;
    }
    else if (etype == element_type_document)
    {
        const uint8_t *ddata = val->data();
        const size_t size = val->size();

        if (scratch->size() < size)
        {
            scratch->resize(size);
        }

        memmove(&scratch->front(), ddata, size);
        *type = HYPERDATATYPE_DOCUMENT;
        *value = e::slice(&(*scratch)[0], size);
        return true;
    }
    else
    {
        // Unsupported BSON type
        return false;
    }
}
