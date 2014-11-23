#ifndef _INCLUDE_DOCUMENT_IMPL_
#define _INCLUDE_DOCUMENT_IMPL_

#include <bson.h>
#include "common/doc/document.h"
#include "common/doc/value.h"

namespace hyperdex
{
namespace doc
{

class document_impl : public hyperdex::doc::document
{
public:
    document_impl(const uint8_t* data, size_t len, bool is_json);
    ~document_impl();

    size_t size() const;
    const uint8_t* data() const;

    bool is_valid() const;

    bool can_create_element(const path& p) const;
    element_type get_element_type(const path& p) const;
    size_t get_num_children(const path& p) const;
    bool does_entry_exist(const path& p) const;

    document* add_or_replace_value(const path& p, const value& val) const throw(std::runtime_error);
    document* unset_value(const path& p) const;
    document* rename_value(const path& p, const std::string& new_name) const;
    value* extract_value(const path& p) const;

    bool overwrite_integer(const path& p, const int64_t new_val) throw(std::runtime_error);
    bool overwrite_float(const path& p, const double new_val) throw(std::runtime_error);

    document* array_prepend(const path& p, const value& new_value) const;
    document* array_append(const path& p, const value& new_value) const;

    const char* c_str() const;

    const char* get_last_error() const;

private:
    void add_or_replace_value_recurse(const path& p, const bson_value_t *new_value,
                                    bson_t* parent, bson_iter_t* iter) const;
    void rename_value_recurse(const path& p, const std::string& new_name, bson_t* parent, bson_iter_t* iter) const;
    void unset_value_recurse(const path& p, bson_t* parent, bson_iter_t* iter) const;

    document_impl(bson_t *bson_);

    bson_t *bson;
    bson_error_t error;
};

}
}

#endif
