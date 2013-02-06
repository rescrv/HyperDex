#include <msclr/marshal.h>
#include "marshal.h"
#include "hyperclientclr.h"
#include <cliext/utility>
#include <client/hyperclient.h>
#include <hyperclientclr.h>
#include <hyperdex.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

using namespace HyperDex;
using namespace System;
using namespace cliext;
using namespace System::Collections::Generic;

#define THROW_EXCEPTION(x) \
    do{ \
        HyperClientException^ ex = gcnew HyperClientException(); \
        ex->return_code = static_cast<HyperClientReturnCode>(x); \
        throw ex; \
    } while (0)


msclr::interop::context_node<const char*, Double ^>::context_node(const char*& toObject, Double ^fromObject)
{
    toPtr = NULL;
    toPtr = (const char*)(new double(*fromObject));
    toObject = toPtr;
}

msclr::interop::context_node<const char*, Double ^>::~context_node()
{
    this->!context_node();
}

msclr::interop::context_node<const char*, Double ^>::!context_node()
{
    if (toPtr != NULL) {
        delete toPtr;
        toPtr = NULL;
    }
}
msclr::interop::context_node<const char*, Int64 ^>::context_node(const char*& toObject, Int64 ^fromObject)
{
    toPtr = NULL;
    toPtr = (const char*)(new int64_t(*fromObject));
    toObject = toPtr;
}

msclr::interop::context_node<const char*, Int64 ^>::~context_node()
{
    this->!context_node();
}

msclr::interop::context_node<const char*, Int64 ^>::!context_node()
{
    if (toPtr != NULL) {
        delete toPtr;
        toPtr = NULL;
    }
}

msclr::interop::context_node<hyperclient_attribute*, Dictionary<String^, Object^>^ >::context_node(hyperclient_attribute*& toObject, Dictionary<String^, Object^>^ fromObject)
{
    size = fromObject->Count;

    toPtr = new hyperclient_attribute[size]();

    int i = 0;
    for each(KeyValuePair<String ^, Object ^> kvp in fromObject)
    {
        if(kvp.Value->GetType() == String::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((String ^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_STRING;
            toPtr[i].value_sz = strlen(toPtr[i].value);
        }
        else if(kvp.Value->GetType() == Int64::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((Int64 ^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_INT64;
            toPtr[i].value_sz = sizeof(int64_t);
        }
        else if(kvp.Value->GetType() == Double::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((Double^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_FLOAT;
            toPtr[i].value_sz = sizeof(double);
        }
        else if (kvp.Value->GetType()->IsGenericType 
                && kvp.Value->GetType()->GetGenericTypeDefinition() == List::typeid)
        {
            if(kvp.Value->GetType()->GetGenericArguments()[0] == String::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_LIST_STRING;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                for each(String ^ s in (List<String^>^)kvp.Value)
                {
                    const char* str = context.marshal_as<const char*>(s);
                    uint32_t len = s->Length;
                    char * val = (char*)malloc(len + sizeof(uint32_t));

                    memcpy(val,&len,sizeof(uint32_t));
                    memcpy(val + sizeof(uint32_t), str, len);
                    toPtr[i].value = val;

                    toPtr[i].value_sz = len + sizeof(uint32_t);
                }
            }
            else if(kvp.Value->GetType()->GetGenericArguments()[0] == Int64::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_LIST_INT64;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                uint32_t len = ((List<Int64>^)kvp.Value)->Count * 8;

                char * val = (char*)malloc(len);

                for each(Int64 s in (List<Int64>^)kvp.Value)
                {
                    int64_t str = s;
                    memcpy(val, &str, sizeof(int64_t));
                    val += sizeof(int64_t);
                }
                    
                toPtr[i].value = val - len;
                toPtr[i].value_sz = len;
            }
            else if(kvp.Value->GetType()->GetGenericArguments()[0] == Double::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_LIST_FLOAT;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                uint32_t len = ((List<Double>^)kvp.Value)->Count * 8;

                char * val = (char*)malloc(len);

                for each(Double s in (List<Double>^)kvp.Value)
                {
                    double str = s;
                    memcpy(val, &str, sizeof(double));
                    val += sizeof(double);
                }
                    
                toPtr[i].value = val - len;
                toPtr[i].value_sz = len;
            }        
        }
        else if (kvp.Value->GetType()->IsGenericType 
                && kvp.Value->GetType()->GetGenericTypeDefinition() == SortedSet::typeid)
        {
            if(kvp.Value->GetType()->GetGenericArguments()[0] == String::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_SET_STRING;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                uint32_t len = 0;
                for each(String ^ s in (SortedSet<String^>^)kvp.Value)
                {
                    len += sizeof(uint32_t) + s->Length;
                }

                char * val = (char*)malloc(len);

                for each(String ^ s in (SortedSet<String^>^)kvp.Value)
                {
                    const char* str = context.marshal_as<const char*>(s);
                    uint32_t sz = s->Length;

                    memcpy(val,&sz,sizeof(uint32_t));
                    val += sizeof(uint32_t);

                    memcpy(val, str, sz);
                    val += sz;
                }
                    
                toPtr[i].value = val - len;
                toPtr[i].value_sz = len;
            }
            else if(kvp.Value->GetType()->GetGenericArguments()[0] == Int64::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_SET_INT64;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                uint32_t len = ((SortedSet<Int64>^)kvp.Value)->Count * 8;

                char * val = (char*)malloc(len);

                for each(Int64 s in (SortedSet<Int64>^)kvp.Value)
                {
                    int64_t str = s;
                    memcpy(val, &str, sizeof(int64_t));
                    val += sizeof(int64_t);
                }
                    
                toPtr[i].value = val - len;
                toPtr[i].value_sz = len;
            }
            else if(kvp.Value->GetType()->GetGenericArguments()[0] == Double::typeid)
            {
                toPtr[i].datatype = HYPERDATATYPE_SET_FLOAT;
                toPtr[i].attr = context.marshal_as<const char*>((String^)kvp.Key);

                uint32_t len = ((SortedSet<Double>^)kvp.Value)->Count * 8;

                char * val = (char*)malloc(len);

                for each(Double s in (SortedSet<Double>^)kvp.Value)
                {
                    double str = s;
                    memcpy(val, &str, sizeof(double));
                    val += sizeof(double);
                }
                    
                toPtr[i].value = val - len;
                toPtr[i].value_sz = len;
            }        
        }
        else
        {
            delete[] toPtr;
            THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
        }

        ++i;
    }

    toObject = toPtr;
}
msclr::interop::context_node<hyperclient_attribute*, Dictionary<String^, Object^>^ >::~context_node()
{
    this->!context_node();
}
msclr::interop::context_node<hyperclient_attribute*, Dictionary<String^, Object^>^ >::!context_node()
{
    if (toPtr != NULL) {
        delete toPtr;
        toPtr = NULL;
    }
}

msclr::interop::context_node<hyperclient_map_attribute*, Dictionary<String^, Dictionary<Object^, Object^>^>^ >::context_node(hyperclient_map_attribute*& toObject, Dictionary<String^, Dictionary<Object^,Object^>^>^ fromObject)
{
    size = 0;
    for each(KeyValuePair<String ^, Dictionary<Object ^, Object ^>^ > attrkvp in fromObject)
    {
        size += attrkvp.Value->Count;
    }


    toPtr = new hyperclient_map_attribute[size]();

    int i = 0;
    for each(KeyValuePair<String ^, Dictionary<Object ^, Object ^>^ > attrkvp in fromObject)
    {
        /* Map Attribute */
        const char* name = context.marshal_as<const char*>((String ^)attrkvp.Key);

        /* Map Keys */
        for each(KeyValuePair<Object ^, Object ^> kvp in attrkvp.Value)
        {
            toPtr[i].attr = name;

            if(kvp.Value->GetType() == String::typeid)
            {
                toPtr[i].value = context.marshal_as<const char*>((String ^)kvp.Value);
                toPtr[i].value_datatype = HYPERDATATYPE_STRING;
                toPtr[i].value_sz = strlen(toPtr[i].value);
            }
            else if(kvp.Value->GetType() == Int64::typeid)
            {
                toPtr[i].value = context.marshal_as<const char*>((Int64 ^)kvp.Value);
                toPtr[i].value_datatype = HYPERDATATYPE_INT64;
                toPtr[i].value_sz = sizeof(int64_t);
            }
            else if(kvp.Value->GetType() == Double::typeid)
            {
                toPtr[i].value = context.marshal_as<const char*>((Double^)kvp.Value);
                toPtr[i].value_datatype = HYPERDATATYPE_FLOAT;
                toPtr[i].value_sz = sizeof(double);
            }
            else
            {
                delete[] toPtr;
                THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
            }

            /* Map Value */
            if(kvp.Key->GetType() == String::typeid)
            {
                toPtr[i].map_key = context.marshal_as<const char*>((String ^)kvp.Key);
                toPtr[i].map_key_datatype = HYPERDATATYPE_STRING;
                toPtr[i].map_key_sz = strlen(toPtr[i].map_key);
            }
            else if(kvp.Key->GetType() == Int64::typeid)
            {
                toPtr[i].map_key = context.marshal_as<const char*>((Int64 ^)kvp.Key);
                toPtr[i].map_key_datatype = HYPERDATATYPE_INT64;
                toPtr[i].map_key_sz = sizeof(int64_t);
            }
            else if(kvp.Key->GetType() == Double::typeid)
            {
                toPtr[i].map_key = context.marshal_as<const char*>((Double ^)kvp.Key);
                toPtr[i].map_key_datatype = HYPERDATATYPE_FLOAT;
                toPtr[i].map_key_sz = sizeof(double);
            }
            else
            {
                delete[] toPtr;
                THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
            }

            ++i;
        }

        ++i;
    }

    toObject = toPtr;
}

msclr::interop::context_node<hyperclient_map_attribute*, Dictionary<String^, Dictionary<Object^, Object^>^ >^ >::~context_node()
{
    this->!context_node();
}
msclr::interop::context_node<hyperclient_map_attribute*, Dictionary<String^, Dictionary<Object^, Object^>^ >^ >::!context_node()
{
    if (toPtr != NULL) {
        delete toPtr;
        toPtr = NULL;
    }
}

msclr::interop::context_node<hyperclient_attribute_check*, Dictionary<String^, Object^>^ >::context_node(hyperclient_attribute_check*& toObject, Dictionary<String^, Object^>^ fromObject)
{
    //Waste a bit of memory but we don't have to cycle the dictionary twice to find out
    //how many of the checks are pairs.
    size = fromObject->Count * 2;

    toPtr = new hyperclient_attribute_check[size]();

    int i = 0;
    for each(KeyValuePair<String ^, Object ^> kvp in fromObject)
    {
        if(kvp.Value->GetType() == String::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((String ^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_STRING;
            toPtr[i].value_sz = strlen(toPtr[i].value);
            toPtr[i].predicate = HYPERPREDICATE_EQUALS;
        }
        else if(kvp.Value->GetType() == Int64::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((Int64 ^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_INT64;
            toPtr[i].value_sz = sizeof(int64_t);
            toPtr[i].predicate = HYPERPREDICATE_EQUALS;
        }
        else if(kvp.Value->GetType() == Double::typeid)
        {
            toPtr[i].value = context.marshal_as<const char*>((Double^)kvp.Value);
            toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
            toPtr[i].datatype = HYPERDATATYPE_FLOAT;
            toPtr[i].value_sz = sizeof(double);
            toPtr[i].predicate = HYPERPREDICATE_EQUALS;
        }
        else if (kvp.Value->GetType()->IsGenericType 
                && kvp.Value->GetType()->GetGenericTypeDefinition() == KeyValuePair::typeid)
        {
            int j = i;
            do
            {
                if(kvp.Value->GetType() == String::typeid)
                {
                    toPtr[i].value = context.marshal_as<const char*>((String ^)kvp.Value);
                    toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
                    toPtr[i].datatype = HYPERDATATYPE_STRING;
                    toPtr[i].value_sz = strlen(toPtr[i].value);
                    if(j == i)
                    {
                        /* First argument is always the lower bound. */
                        toPtr[i].predicate = HYPERPREDICATE_GREATER_EQUAL;
                    }
                    else
                    {
                        /* Second argument is always the lower bound. */
                        toPtr[i].predicate = HYPERPREDICATE_LESS_EQUAL;
                    }
                }
                else if(kvp.Value->GetType() == Int64::typeid)
                {
                    toPtr[i].value = context.marshal_as<const char*>((Int64 ^)kvp.Value);
                    toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
                    toPtr[i].datatype = HYPERDATATYPE_INT64;
                    toPtr[i].value_sz = sizeof(int64_t);
                    if(j == i)
                    {
                        toPtr[i].predicate = HYPERPREDICATE_GREATER_EQUAL;
                    }
                    else
                    {
                        toPtr[i].predicate = HYPERPREDICATE_LESS_EQUAL;
                    }
                }
                else if(kvp.Value->GetType() == Double::typeid)
                {
                    toPtr[i].value = context.marshal_as<const char*>((Double^)kvp.Value);
                    toPtr[i].attr = context.marshal_as<const char*>((String ^)kvp.Key);
                    toPtr[i].datatype = HYPERDATATYPE_FLOAT;
                    toPtr[i].value_sz = sizeof(double);
                    if(j == i)
                    {
                        toPtr[i].predicate = HYPERPREDICATE_GREATER_EQUAL;
                    }
                    else
                    {
                        toPtr[i].predicate = HYPERPREDICATE_LESS_EQUAL;
                    }
                }
                else
                {
                    delete[] toPtr;
                    THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
                }
            }
            while(i < j + 1 && ++i);
        }

        else
        {
            delete[] toPtr;
            THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
        }

        ++i;
    }

    toObject = toPtr;
}
msclr::interop::context_node<hyperclient_attribute_check*, Dictionary<String^, Object^>^ >::~context_node()
{
    this->!context_node();
}
msclr::interop::context_node<hyperclient_attribute_check*, Dictionary<String^, Object^>^ >::!context_node()
{
    if (toPtr != NULL) {
        delete toPtr;
        toPtr = NULL;
    }
}
