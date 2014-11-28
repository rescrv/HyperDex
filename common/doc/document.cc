#include "document_impl.h"

namespace hyperdex
{
namespace doc
{

document* create_document(const uint8_t* data, size_t len)
{
    return new document_impl(data, len, false);
}

document* create_document_from_json(const uint8_t* data, size_t len)
{
    return new document_impl(data, len, true);
}

}
}
