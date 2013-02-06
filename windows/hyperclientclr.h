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
        HYPERCLIENT_SUCCESS      = 8448,
                                 HYPERCLIENT_NOTFOUND     = 8449,
                                 HYPERCLIENT_SEARCHDONE   = 8450,
                                 HYPERCLIENT_CMPFAIL      = 8451,
                                 HYPERCLIENT_READONLY     = 8452,

                                 /* Error conditions */
                                 HYPERCLIENT_UNKNOWNSPACE = 8512,
                                 HYPERCLIENT_COORDFAIL    = 8513,
                                 HYPERCLIENT_SERVERERROR  = 8514,
                                 HYPERCLIENT_POLLFAILED   = 8515,
                                 HYPERCLIENT_OVERFLOW     = 8516,
                                 HYPERCLIENT_RECONFIGURE  = 8517,
                                 HYPERCLIENT_TIMEOUT      = 8519,
                                 HYPERCLIENT_UNKNOWNATTR  = 8520,
                                 HYPERCLIENT_DUPEATTR     = 8521,
                                 HYPERCLIENT_NONEPENDING  = 8523,
                                 HYPERCLIENT_DONTUSEKEY   = 8524,
                                 HYPERCLIENT_WRONGTYPE    = 8525,
                                 HYPERCLIENT_NOMEM        = 8526,
                                 HYPERCLIENT_BADCONFIG    = 8527,
                                 HYPERCLIENT_BADSPACE     = 8528,
                                 HYPERCLIENT_DUPLICATE    = 8529,
                                 HYPERCLIENT_INTERRUPTED  = 8530,
                                 HYPERCLIENT_CLUSTER_JUMP = 8531,
                                 HYPERCLIENT_COORD_LOGGED = 8532,

                                 /* This should never happen.  It indicates a bug */
                                 HYPERCLIENT_INTERNAL     = 8573,
                                 HYPERCLIENT_EXCEPTION    = 8574,
                                 HYPERCLIENT_GARBAGE      = 8575
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

#define HYPERCLIENT_CLRDEC(OPNAME) \
        bool OPNAME(String ^space, String ^key, Dictionary<String ^, Object ^> ^ attrs); 

        HYPERCLIENT_CLRDEC(put)
            HYPERCLIENT_CLRDEC(put_if_not_exist)
            HYPERCLIENT_CLRDEC(atomic_add)
            HYPERCLIENT_CLRDEC(atomic_sub)
            HYPERCLIENT_CLRDEC(atomic_mul)
            HYPERCLIENT_CLRDEC(atomic_div)
            HYPERCLIENT_CLRDEC(atomic_mod)
            HYPERCLIENT_CLRDEC(atomic_and)
            HYPERCLIENT_CLRDEC(atomic_or)
            HYPERCLIENT_CLRDEC(atomic_xor)
            HYPERCLIENT_CLRDEC(string_prepend)
            HYPERCLIENT_CLRDEC(string_append)
            HYPERCLIENT_CLRDEC(list_lpush)
            HYPERCLIENT_CLRDEC(list_rpush)
            HYPERCLIENT_CLRDEC(set_add)
            HYPERCLIENT_CLRDEC(set_remove)
            HYPERCLIENT_CLRDEC(set_intersect)
            HYPERCLIENT_CLRDEC(set_union)

#define HYPERCLIENT_MAP_CLRDEC(OPNAME) \
            bool map_ ## OPNAME(String ^space, String ^key, Dictionary<String ^, Dictionary<Object^, Object ^>^ > ^ attrs); 

            HYPERCLIENT_MAP_CLRDEC(add)
            HYPERCLIENT_MAP_CLRDEC(remove)
            HYPERCLIENT_MAP_CLRDEC(atomic_add)
            HYPERCLIENT_MAP_CLRDEC(atomic_sub)
            HYPERCLIENT_MAP_CLRDEC(atomic_mul)
            HYPERCLIENT_MAP_CLRDEC(atomic_div)
            HYPERCLIENT_MAP_CLRDEC(atomic_mod)
            HYPERCLIENT_MAP_CLRDEC(atomic_and)
            HYPERCLIENT_MAP_CLRDEC(atomic_or)
            HYPERCLIENT_MAP_CLRDEC(atomic_xor)
            HYPERCLIENT_MAP_CLRDEC(string_prepend)
            HYPERCLIENT_MAP_CLRDEC(string_append)

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
