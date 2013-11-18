#pragma once
#include <client/hyperclient.h>
#include <stdlib.h>
#include <string.h>
#include <msclr/marshal.h>
#include <cliext/utility>
#include <marshal.h>

using namespace msclr::interop;
using namespace cliext;
using namespace System;
using namespace System::Collections::Generic;

#define THROW_EXCEPTION(x) \
    do{ \
        HyperClientException^ ex = gcnew HyperClientException(); \
        ex->return_code = static_cast<HyperClientReturnCode>(x); \
        throw ex; \
    } while (0)

namespace
HyperDex
{
    public enum class HyperClientReturnCode
    {
        HYPERDEX_CLIENT_SUCCESS      = 8448,
                                 HYPERDEX_CLIENT_NOTFOUND     = 8449,
                                 HYPERDEX_CLIENT_SEARCHDONE   = 8450,
                                 HYPERDEX_CLIENT_CMPFAIL      = 8451,
                                 HYPERDEX_CLIENT_READONLY     = 8452,

                                 /* Error conditions */
                                 HYPERDEX_CLIENT_UNKNOWNSPACE = 8512,
                                 HYPERDEX_CLIENT_COORDFAIL    = 8513,
                                 HYPERDEX_CLIENT_SERVERERROR  = 8514,
                                 HYPERDEX_CLIENT_POLLFAILED   = 8515,
                                 HYPERDEX_CLIENT_OVERFLOW     = 8516,
                                 HYPERDEX_CLIENT_RECONFIGURE  = 8517,
                                 HYPERDEX_CLIENT_TIMEOUT      = 8519,
                                 HYPERDEX_CLIENT_UNKNOWNATTR  = 8520,
                                 HYPERDEX_CLIENT_DUPEATTR     = 8521,
                                 HYPERDEX_CLIENT_NONEPENDING  = 8523,
                                 HYPERDEX_CLIENT_DONTUSEKEY   = 8524,
                                 HYPERDEX_CLIENT_WRONGTYPE    = 8525,
                                 HYPERDEX_CLIENT_NOMEM        = 8526,
                                 HYPERDEX_CLIENT_BADCONFIG    = 8527,
                                 HYPERDEX_CLIENT_BADSPACE     = 8528,
                                 HYPERDEX_CLIENT_DUPLICATE    = 8529,
                                 HYPERDEX_CLIENT_INTERRUPTED  = 8530,
                                 HYPERDEX_CLIENT_CLUSTER_JUMP = 8531,
                                 HYPERDEX_CLIENT_COORD_LOGGED = 8532,

                                 /* This should never happen.  It indicates a bug */
                                 HYPERDEX_CLIENT_INTERNAL     = 8573,
                                 HYPERDEX_CLIENT_EXCEPTION    = 8574,
                                 HYPERDEX_CLIENT_GARBAGE      = 8575
    };

    public ref struct HyperClientException : public System::Exception
    {
        public:
            HyperClientReturnCode^ return_code;
    };

    public ref class HyperClient
    {

        hyperclient* _hc;
        marshal_context^ mc;

        public:
        HyperClient(String^ coordinator, uint16_t port);
        ~HyperClient();

        protected:
        !HyperClient();

        public:
        Dictionary<String ^, Object ^> ^get(String ^space, String ^key);

#define HYPERDEX_CLIENT_CLRDEC(OPNAME) \
        bool OPNAME(String ^space, String ^key, Dictionary<String ^, Object ^> ^ attrs);

        HYPERDEX_CLIENT_CLRDEC(put)
            HYPERDEX_CLIENT_CLRDEC(put_if_not_exist)
            HYPERDEX_CLIENT_CLRDEC(atomic_add)
            HYPERDEX_CLIENT_CLRDEC(atomic_sub)
            HYPERDEX_CLIENT_CLRDEC(atomic_mul)
            HYPERDEX_CLIENT_CLRDEC(atomic_div)
            HYPERDEX_CLIENT_CLRDEC(atomic_mod)
            HYPERDEX_CLIENT_CLRDEC(atomic_and)
            HYPERDEX_CLIENT_CLRDEC(atomic_or)
            HYPERDEX_CLIENT_CLRDEC(atomic_xor)
            HYPERDEX_CLIENT_CLRDEC(string_prepend)
            HYPERDEX_CLIENT_CLRDEC(string_append)
            HYPERDEX_CLIENT_CLRDEC(list_lpush)
            HYPERDEX_CLIENT_CLRDEC(list_rpush)
            HYPERDEX_CLIENT_CLRDEC(set_add)
            HYPERDEX_CLIENT_CLRDEC(set_remove)
            HYPERDEX_CLIENT_CLRDEC(set_intersect)
            HYPERDEX_CLIENT_CLRDEC(set_union)

#define HYPERDEX_CLIENT_MAP_CLRDEC(OPNAME) \
            bool map_ ## OPNAME(String ^space, String ^key, Dictionary<String ^, Dictionary<Object^, Object ^>^ > ^ attrs);

            HYPERDEX_CLIENT_MAP_CLRDEC(add)
            HYPERDEX_CLIENT_MAP_CLRDEC(remove)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_add)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_sub)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_mul)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_div)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_mod)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_and)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_or)
            HYPERDEX_CLIENT_MAP_CLRDEC(atomic_xor)
            HYPERDEX_CLIENT_MAP_CLRDEC(string_prepend)
            HYPERDEX_CLIENT_MAP_CLRDEC(string_append)

            List<Dictionary<String ^, Object^>^ >^
            search(String^ space, Dictionary<String ^, Object^>^ checks);

        List<Dictionary<String ^, Object^>^ >^
            sorted_search(String^ space, String ^sortby,
                    UInt64 limit, bool maximize, Dictionary<String ^, Object^>^ checks);


        UInt64
            count(String^ space, Dictionary<String ^, Object^>^ checks);


        bool
            del(String^ space, String^ key);
    };
}
