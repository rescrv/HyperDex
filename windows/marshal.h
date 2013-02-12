#pragma once

#include <msclr/marshal.h>
#include <cliext/utility>
#include <client/hyperclient.h>
#include <hyperclientclr.h>
#include <hyperdex.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

using namespace System;
using namespace cliext;
using namespace System::Collections::Generic;

#define THROW_EXCEPTION(x) \
    do{ \
    HyperClientException^ ex = gcnew HyperClientException(); \
    ex->return_code = static_cast<HyperClientReturnCode>(x); \
    throw ex; \
    } while (0)


namespace msclr {
    namespace interop {
        template<>
            ref class context_node<const char*, Double ^> : public context_node_base
            {
                private:
                    const char* toPtr;
                    marshal_context context;

                public:
                    context_node(const char*& toObject, Double ^fromObject);
                    ~context_node();

                protected:
                    !context_node();
            };

        template<>
            ref class context_node<const char*, Int64 ^> : public context_node_base
            {
                private:
                    const char* toPtr;
                    marshal_context context;

                public:
                    context_node(const char*& toObject, Int64 ^fromObject);
                    ~context_node();

                protected:
                    !context_node();
            };

        template<>
            ref class context_node<hyperclient_attribute*, Dictionary<String^, Object^>^ > : public context_node_base
            {
                private:
                    hyperclient_attribute* toPtr;
                    int size;
                    marshal_context context;
                public:
                    context_node(hyperclient_attribute*& toObject, Dictionary<String^, Object^>^ fromObject);
                    ~context_node();
                protected:
                    !context_node();
            };

        template<>
            ref class context_node<hyperclient_map_attribute*, Dictionary<String^, Dictionary<Object^, Object^>^ >^ > : public context_node_base
            {
                private:
                    hyperclient_map_attribute* toPtr;
                    int size;
                    marshal_context context;
                public:
                    context_node(hyperclient_map_attribute*& toObject,Dictionary<String^, Dictionary<Object^, Object^>^ >^ fromObject);
                    ~context_node();
                protected:
                    !context_node();
            };

        template<>
            ref class context_node<hyperclient_attribute_check*, Dictionary<String^, Object^>^ > : public context_node_base
            {
                private:
                    hyperclient_attribute_check* toPtr;
                    int size;
                    marshal_context context;
                public:
                    context_node(hyperclient_attribute_check*& toObject, Dictionary<String^, Object^>^ fromObject);
                    ~context_node();
                protected:
                    !context_node();
            };
    }
}
