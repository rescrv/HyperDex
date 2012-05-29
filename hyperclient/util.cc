// Copyright (c) 2011-2012, Cornell University
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

// e
#include <e/endian.h>

// HyperClient
#include "hyperclient/util.h"

bool
value_to_attributes(const hyperdex::configuration& config,
                    const hyperdex::entityid& entity,
                    const uint8_t* key,
                    size_t key_sz,
                    const std::vector<e::slice>& value,
                    hyperclient_returncode* loop_status,
                    hyperclient_returncode* op_status,
                    hyperclient_attribute** attrs,
                    size_t* attrs_sz)
{
    *loop_status = HYPERCLIENT_SUCCESS;
    std::vector<hyperdex::attribute> dimension_names = config.dimension_names(entity.get_space());

    if (value.size() + 1 != dimension_names.size())
    {
        *op_status = HYPERCLIENT_SERVERERROR;
        return false;
    }

    size_t sz = sizeof(hyperclient_attribute) * dimension_names.size() + key_sz
              + dimension_names[0].name.size() + 1;

    for (size_t i = 0; i < value.size(); ++i)
    {
        sz += dimension_names[i + 1].name.size() + 1 + value[i].size();
    }

    std::vector<hyperclient_attribute> ha;
    ha.reserve(dimension_names.size());
    char* ret = static_cast<char*>(malloc(sz));

    if (!ret)
    {
        *loop_status = HYPERCLIENT_NOMEM;
        return false;
    }

    e::guard g = e::makeguard(free, ret);
    char* data = ret + sizeof(hyperclient_attribute) * value.size();

    if (key)
    {
        data += sizeof(hyperclient_attribute);
        ha.push_back(hyperclient_attribute());
        size_t attr_sz = dimension_names[0].name.size() + 1;
        ha.back().attr = data;
        memmove(data, dimension_names[0].name.c_str(), attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, key, key_sz);
        data += key_sz;
        ha.back().value_sz = key_sz;
        ha.back().datatype = dimension_names[0].type;
    }

    for (size_t i = 0; i < value.size(); ++i)
    {
        ha.push_back(hyperclient_attribute());
        size_t attr_sz = dimension_names[i + 1].name.size() + 1;
        ha.back().attr = data;
        memmove(data, dimension_names[i + 1].name.c_str(), attr_sz);
        data += attr_sz;
        ha.back().value = data;
        memmove(data, value[i].data(), value[i].size());
        data += value[i].size();
        ha.back().value_sz = value[i].size();
        ha.back().datatype = dimension_names[i + 1].type;
    }

    memmove(ret, &ha.front(), sizeof(hyperclient_attribute) * ha.size());
    *op_status = HYPERCLIENT_SUCCESS;
    *attrs = reinterpret_cast<hyperclient_attribute*>(ret);
    *attrs_sz = ha.size();
    g.dismiss();
    return true;
}

bool
compare_for_microop_sort(const hyperdex::microop& lhs,
                         const hyperdex::microop& rhs)
{
   if (lhs.attr < rhs.attr)
   {
      return true;
   }
   if (lhs.attr > rhs.attr)
   {
      return false;
   }

   int64_t lhsi;
   int64_t rhsi;

   switch (lhs.type)
   {
      case HYPERDATATYPE_STRING:
      case HYPERDATATYPE_INT64:
      case HYPERDATATYPE_LIST_STRING:
      case HYPERDATATYPE_LIST_INT64:
      case HYPERDATATYPE_SET_STRING:
      case HYPERDATATYPE_SET_INT64:
         return false;
      case HYPERDATATYPE_MAP_STRING_KEYONLY:
      case HYPERDATATYPE_MAP_STRING_STRING:
      case HYPERDATATYPE_MAP_STRING_INT64:
         return lhs.arg2 < rhs.arg2;
      case HYPERDATATYPE_MAP_INT64_KEYONLY:
      case HYPERDATATYPE_MAP_INT64_STRING:
      case HYPERDATATYPE_MAP_INT64_INT64:
         e::unpack64le(lhs.arg2.data(), &lhsi);
         e::unpack64le(rhs.arg2.data(), &rhsi);
         return lhsi < rhsi;
      case HYPERDATATYPE_LIST_GENERIC:
      case HYPERDATATYPE_SET_GENERIC:
      case HYPERDATATYPE_MAP_GENERIC:
      case HYPERDATATYPE_GARBAGE:
      default:
         abort();
   }
}
