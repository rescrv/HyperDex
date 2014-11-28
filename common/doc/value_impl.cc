#include "value_impl.h"
#include <stdexcept>

namespace hyperdex
{
namespace doc
{

const char*
value_impl :: get_string() const
{
    throw std::runtime_error("Operation not supported");
}

int
value_impl :: get_int() const
{
    throw std::runtime_error("Operation not supported");
}

double
value_impl :: get_float() const
{
    throw std::runtime_error("Operation not supported");
}

const uint8_t*
value_impl :: data() const
{
    throw std::runtime_error("Operation not supported");
}

size_t
value_impl :: size() const
{
    throw std::runtime_error("Operation not supported");
}

bson_value_t value_int::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_INT64;
    bvalue.value.v_int64 = value;

    return bvalue;
}

bson_value_t value_string::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_UTF8;
    bvalue.value.v_utf8.str = const_cast<char*>(value);
    bvalue.value.v_utf8.len = len;

    return bvalue;
}

bson_value_t value_bool::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_BOOL;
    bvalue.value.v_bool = value;

    return bvalue;
}

bson_value_t value_float::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_DOUBLE;
    bvalue.value.v_double = value;

    return bvalue;
}

value_bson_document::value_bson_document(const uint8_t* data_, size_t size_)
    : mdata(data_), msize(size_)
{
}

const uint8_t*
value_bson_document :: data() const
{
    return mdata;
}

size_t
value_bson_document :: size() const
{
    return msize;
}

bson_value_t value_bson_document::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_DOCUMENT;
    bvalue.value.v_doc.data = (uint8_t*)mdata;
    bvalue.value.v_doc.data_len = msize;

    return bvalue;
}

value_bson_array::value_bson_array(const uint8_t* data_, size_t size_)
    : mdata(data_), msize(size_)
{
}

const uint8_t*
value_bson_array :: data() const
{
    return mdata;
}

size_t
value_bson_array :: size() const
{
    return msize;
}

bson_value_t value_bson_array::to_bson() const
{
    bson_value_t bvalue;
    bvalue.value_type = BSON_TYPE_ARRAY;
    bvalue.value.v_doc.data = (uint8_t*)mdata;
    bvalue.value.v_doc.data_len = msize;

    return bvalue;
}

}
}
