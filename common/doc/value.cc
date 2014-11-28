#include <stdexcept>
#include "common/doc/value.h"
#include "common/doc/value_impl.h"

namespace hyperdex
{
namespace doc
{

value* create_value(double fp)
{
    return new value_float(fp);
}

value* create_value(int64_t integer)
{
    return new value_int(integer);
}

value* create_value(const char* str, const size_t len)
{
    return new value_string(str, len);
}

value* create_value(bool boolean)
{
    return new value_bool(boolean);
}

value* create_document_value(const uint8_t* data, size_t size)
{
    return new value_bson_document(data, size);
}

value* create_array_value(const uint8_t* data, size_t size)
{
    return new value_bson_array(data, size);
}

}
}
