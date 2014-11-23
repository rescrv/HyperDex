#ifndef _INCLUDE_DOCUMENT_DOCUMENT_
#define _INCLUDE_DOCUMENT_DOCUMENT_

#include <stdexcept>
#include "common/doc/element_type.h"
#include "common/doc/path.h"
#include "common/doc/value.h"

namespace hyperdex
{
namespace doc
{

// An interface to a binary document optimized for performance
// This means it doesn't have a DOM-style interface but works on a blog of binary data
class document
{
public:
    virtual ~document() {};

    virtual size_t size() const = 0;
    virtual const uint8_t* data() const = 0;

    // Is this a valid document? Call after creation to check if parsing went fine
    virtual bool is_valid() const = 0;

    // Get the last error
    // This might only be useful if parsing the binary data failed
    virtual const char* get_last_error() const = 0;

    virtual bool can_create_element(const path& p) const = 0;
    virtual element_type get_element_type(const path& p) const = 0;
    virtual size_t get_num_children(const path& p) const = 0;
    virtual bool does_entry_exist(const path& p) const = 0;

    // Creates a new document by replaceing (or adding) a value
    // You have to free the memory of the current document
    virtual document* add_or_replace_value(const path& p, const value& val) const = 0;

    // Remove an entry from the document
    virtual document* unset_value(const path& p) const = 0;

    // Rename an entry to a new name (note: name must not be a path)
    virtual document* rename_value(const path& p, const std::string& new_name) const = 0;

    // Extract the value located at path p
    virtual value* extract_value(const path& p) const = 0;

    // Add a new value to the front of an array
    virtual document* array_prepend(const path& p, const value& new_value) const = 0;

    // Append a new value to an array
    virtual document* array_append(const path& p, const value& new_value) const = 0;

    // Overwrite an integer, without rewriting the document
    virtual bool overwrite_integer(const path& p, const int64_t new_val) = 0;

    // Overwrite a float, without rewriting the document
    virtual bool overwrite_float(const path& p, const double new_val) = 0;

    // Get content in a human-readable from (JSON)
    virtual const char* c_str() const = 0;

};

// Create a document representation from an arbitary buffer
document* create_document(const uint8_t* data, size_t len);

// Create a document from the string representation
document* create_document_from_json(const uint8_t* data, size_t len);

}
}

#endif //header guard
