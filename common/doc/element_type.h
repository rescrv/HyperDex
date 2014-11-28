#ifndef _INCLUDE_DOCUMENT_ELEMENT_TYPE_
#define _INCLUDE_DOCUMENT_ELEMENT_TYPE_

namespace hyperdex
{
namespace doc
{

enum element_type
{
    element_type_null = 0,
    element_type_document,
    element_type_array,
    element_type_string,
    element_type_integer,
    element_type_float,
    element_type_bool
};

}
}

#endif
