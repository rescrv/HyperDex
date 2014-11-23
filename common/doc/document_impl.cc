#include "common/doc/document_impl.h"
#include "common/doc/value_impl.h"

#include <sstream>
#include <vector>
#include <memory>

namespace hyperdex
{
namespace doc
{

bool is_integer(const std::string& str)
{
	for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
	{
		if (!isdigit(*it))
		{
			return false;
		}
	}

	// Non-empty and all chars are digits
	return str.size() > 0;
}

value* to_generic_value(const bson_value_t val)
{
    bson_type_t type = val.value_type;

    if(type == BSON_TYPE_DOCUMENT)
    {
        uint32_t size = val.value.v_doc.data_len;
        const uint8_t *data = val.value.v_doc.data;

        return create_document_value(data, size);
    }
    else if(type == BSON_TYPE_ARRAY)
    {
        uint32_t size = val.value.v_doc.data_len;
        const uint8_t *data = val.value.v_doc.data;

        return create_array_value(data, size);
    }
    else if(type == BSON_TYPE_UTF8)
    {
        const char* str = val.value.v_utf8.str;
        const size_t len = val.value.v_utf8.len;

        return create_value(str, len);
    }
    else if(type == BSON_TYPE_INT64)
    {
        int64_t i = val.value.v_int64;
        return create_value(i);
    }
    else if(type == BSON_TYPE_DOUBLE)
    {
        double d = val.value.v_double;
        return create_value(d);
    }
    else if(type == BSON_TYPE_BOOL)
    {
        bool b = val.value.v_bool;
        return create_value(b);
    }
    else
    {
        throw std::runtime_error("Unknown value type");
    }
}

element_type to_element_type(const bson_type_t type)
{
    if(type == BSON_TYPE_DOCUMENT)
    {
        return element_type_document;
    }
    else if(type == BSON_TYPE_ARRAY)
    {
        return element_type_array;
    }
    else if(type == BSON_TYPE_UTF8)
    {
        return element_type_string;
    }
    else if(type == BSON_TYPE_INT64)
    {
        return element_type_integer;
    }
    else if(type == BSON_TYPE_DOUBLE)
    {
        return element_type_float;
    }
    else if(type == BSON_TYPE_BOOL)
    {
        return element_type_bool;
    }
    else
    {
        throw std::runtime_error("Unknown element type");
    }
}

document_impl :: document_impl(const uint8_t* data, size_t len, bool is_json)
{
    if(!is_json)
    {
        bson = bson_new_from_data(data, len);
    }
    else
    {
         bson = bson_new_from_json(data, len, &error);
    }
}

document_impl :: document_impl(bson_t* bson_) : bson(bson_)
{}

document_impl :: ~document_impl()
{
    bson_destroy(bson);
}

bool
document_impl :: is_valid() const
{
    return (bson != NULL);
}

const char* document_impl :: c_str() const
{
    size_t outsize = 0;
    return bson_as_json(bson, &outsize);
}

const char*
document_impl :: get_last_error() const
{
    return error.message;
}

size_t
document_impl :: get_num_children(const path& p) const
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Invalid Document");
    }

    if(!bson_iter_find_descendant(&iter, p.c_str(), &baz))
    {
        throw std::runtime_error("No such element");
    }

    if(!BSON_ITER_HOLDS_ARRAY(&baz) && !BSON_ITER_HOLDS_DOCUMENT(&baz))
    {
        throw std::runtime_error("Cannot count children of element that isn't document or array.");
    }

    size_t count = 0;
    bson_iter_t sub_iter;
    bson_iter_recurse(&iter, &sub_iter);

    while(bson_iter_next(&sub_iter))
    {
        count++;
    }

    return count;
}

value*
document_impl :: extract_value(const path& p) const
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Invalid document");
    }

    if(!bson_iter_find_descendant(&iter, p.c_str(), &baz))
    {
        return NULL;
    }

    const bson_value_t* val = bson_iter_value(&baz);
    value *result = to_generic_value(*val);

    return result;
}

element_type
document_impl :: get_element_type(const path& p) const
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Invalid document");
    }

    if(!bson_iter_find_descendant(&iter, p.c_str(), &baz))
    {
        return element_type_null;
    }

    bson_type_t type = bson_iter_type(&baz);
    return to_element_type(type);
}

bool
document_impl :: can_create_element(const path& p_) const
{
    if(does_entry_exist(p_))
    {
        return false;
    }

    path p = p_;

    while(!p.empty() && p.has_subtree())
    {
        std::string child_name;
        p.split_reverse(p, child_name);

        bson_iter_t iter, baz;
        bson_iter_init (&iter, bson);
        if(bson_iter_find_descendant (&iter, p.c_str(), &baz))
        {
            // An integer can be a document key or an array index
            if(is_integer(child_name) && BSON_ITER_HOLDS_ARRAY(&baz))
            {
                return true;
            }
            else
            {
                return BSON_ITER_HOLDS_DOCUMENT(&baz);
            }
        }
    }

    // empty document
    return true;
}

size_t
document_impl :: size() const
{
    return bson->len;
}

const uint8_t*
document_impl :: data() const
{
    return bson_get_data(bson);
}

bool
document_impl :: does_entry_exist(const path& p) const
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        return false;
    }

    return bson_iter_find_descendant(&iter, p.c_str(), &baz);
}

document*
document_impl :: array_prepend(const path& p, const value& new_value) const
{
    size_t size = get_num_children(p);
    std::vector<bson_value_t> values;
    values.resize(size);

    // Extract all values from the array
    bson_iter_t iter, baz1, baz2;

    if(!bson_iter_init (&iter, bson))
    {
        abort();
    }

    if(!bson_iter_find_descendant (&iter, p.c_str(), &baz1))
    {
        return false;
    }

    bson_iter_recurse(&baz1, &baz2);
    for(size_t i = 0; i < size; ++i)
    {
        bson_iter_next(&baz2);
        values[i] = *bson_iter_value(&baz2);
    }

    // Build new array
    // TODO make this faster...
    const document *prev = this;

    for(size_t i = 0; i < size + 1; ++i)
    {
        std::stringstream key;
        key << i;

        value *val = NULL;

        if(i == 0)
            val = const_cast<value*>(&new_value);
        else
            val = to_generic_value(values[i-1]);

        path subpath = p;
        subpath.append(key.str());
        const document* new_doc = prev->add_or_replace_value(subpath, *val);

        if(prev != this)
        {
            delete prev;
        }

        if(i != 0)
        {
            delete val;
        }

        prev = new_doc;
    }

    // This is a new copy so we can safely remove const
    return const_cast<document*>(prev);
}

document*
document_impl :: array_append(const path& p, const value& new_value) const
{
    size_t size = get_num_children(p);

    std::stringstream key;
    key << size;

    path new_path = p;
    new_path.append(key.str());

    return add_or_replace_value(new_path, new_value);
}

document*
document_impl :: add_or_replace_value(const path& p, const value& new_value) const throw(std::runtime_error)
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Cannot add or replace value: Invalid document");
    }

    bson_value_t new_bson_value = reinterpret_cast<const value_impl*>(&new_value)->to_bson();

    bson_t *new_doc = bson_new();
    add_or_replace_value_recurse(p, &new_bson_value, new_doc, &iter);
    return new document_impl(new_doc);
}

bool
document_impl :: overwrite_integer(const path& p, const int64_t new_val) throw(std::runtime_error)
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Cannot overwrite integer: Invalid document");
    }

    if(!bson_iter_find_descendant(&iter, p.c_str(), &baz))
    {
        return false;
    }

    if(!BSON_ITER_HOLDS_INT64(&baz))
    {
        return false;
    }

    bson_iter_overwrite_int64(&baz, new_val);
    return true;
}

bool
document_impl :: overwrite_float(const path& p, const double new_val) throw(std::runtime_error)
{
    bson_iter_t iter, baz;

    if(!bson_iter_init (&iter, bson))
    {
        throw std::runtime_error("Cannot overwrite float: Invalid document");
    }

    if(!bson_iter_find_descendant(&iter, p.c_str(), &baz))
    {
        return false;
    }

    if(!BSON_ITER_HOLDS_DOUBLE(&baz))
    {
        return false;
    }

    bson_iter_overwrite_double(&baz, new_val);
    return true;
}

void
document_impl :: add_or_replace_value_recurse(const path& p, const bson_value_t *new_value,
                                    bson_t* parent, bson_iter_t* iter) const
{
    bool found = false;

    // There might be no iterator if we have to create the subtree
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(type == BSON_TYPE_DOCUMENT)
        {
            path subpath;
            std::string root_name;

            if(p.has_subtree())
            {
                assert(p.split(root_name, subpath));
            }
            else
            {
                root_name = p.str();
            }

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();

            if(root_name == key)
            {
                found = true;

                if(p.has_subtree())
                {
                    bson_append_document_begin(parent, key.c_str(), key.size(), child);
                    add_or_replace_value_recurse(subpath, new_value, child, &sub_iter);
                    bson_append_document_end(parent, child);
                }
                else
                {
                    bson_append_value(parent, key.c_str(), key.size(), new_value);
                }
            }
            else
            {
               const bson_value_t *value = bson_iter_value(iter);
               bson_append_value(parent, key.c_str(), key.size(), value);
            }
        }
        else if (type == BSON_TYPE_ARRAY)
        {
            path subpath;
            std::string root_name;

            if(p.has_subtree())
            {
                assert(p.split(root_name, subpath));
            }
            else
            {
                root_name = p.str();
            }

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();

            if(root_name == key)
            {
                found = true;

                if(p.has_subtree())
                {
                    bson_append_array_begin(parent, key.c_str(), key.size(), child);
                    add_or_replace_value_recurse(subpath, new_value, child, &sub_iter);
                    bson_append_array_end(parent, child);
                }
                else
                {
                    bson_append_value(parent, key.c_str(), key.size(), new_value);
                }
            }
            else
            {
               const bson_value_t *value = bson_iter_value(iter);
               bson_append_value(parent, key.c_str(), key.size(), value);
            }
        }
        else
        {
            if(!p.has_subtree() && key == p.str())
            {
                found = true;
                bson_append_value(parent, key.c_str(), key.size(), new_value);
            }
            else
            {
                const bson_value_t *value = bson_iter_value(iter);
                bson_append_value(parent, key.c_str(), key.size(), value);
            }
        }
    }

    if(!found && !p.empty())
    {
        if(p.has_subtree())
        {
            path subpath;
            std::string root_name;
            p.split(root_name, subpath);

            bson_t *child = bson_new();

            bson_append_document_begin(parent, root_name.c_str(), root_name.size(), child);
            add_or_replace_value_recurse(subpath, new_value, child, NULL);
            bson_append_document_end(parent, child);
        }
        else
        {
            bson_append_value(parent, p.c_str(), p.str().size(), new_value);
        }
    }
}

document*
document_impl :: rename_value(const path& p, const std::string& new_name) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, bson))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    rename_value_recurse(p, new_name, new_doc, &iter);
    return new document_impl(new_doc);
}

void
document_impl :: rename_value_recurse(const path& p, const std::string& new_name, bson_t* parent, bson_iter_t* iter) const
{
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(key == p.str())
        {
            // found it!
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, new_name.c_str(), new_name.size(), value);
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            path subpath;
            std::string root_name;

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(p.has_subtree() && p.split(root_name, subpath)
                && root_name == key)
            {
                rename_value_recurse(subpath, new_name, child, &sub_iter);
            }
            else
            {
                // This is not the path you are looking for...
                rename_value_recurse("", "", child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
    }
}

document*
document_impl :: unset_value(const path& p) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, bson))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    unset_value_recurse(p, new_doc, &iter);
    return new document_impl(new_doc);
}

void
document_impl :: unset_value_recurse(const path& p, bson_t* parent, bson_iter_t* iter) const
{
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(key == p.str())
        {
            // skip
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            path subpath;
            std::string root_name;

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(p.has_subtree() && p.split(root_name, subpath)
                && root_name == key)
            {
                unset_value_recurse(subpath, child, &sub_iter);
            }
            else
            {
                // This is not the path you are looking for...
                unset_value_recurse("", child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
    }
}

}
}
