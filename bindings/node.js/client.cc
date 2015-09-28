// Copyright (c) 2013, Cornell University
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

// C
#include <cstring>
#include <cstdlib>

// STL
#include <map>
#include <vector>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "hyperdex/client.h"
#include "hyperdex/datastructures.h"

// Node
#include <node.h>
#include <node_buffer.h>

#pragma GCC visibility push(hidden)

#define HYPERDEX_NODE_INCLUDED_CLIENT_CC

namespace hyperdex
{
namespace nodejs
{

class Operation;

class HyperDexClient : public node::ObjectWrap
{
    public:
        static v8::Persistent<v8::Function> ctor;
        static v8::Persistent<v8::Function> global_err;
        static v8::Persistent<v8::Function> type_hint;
        static v8::Persistent<v8::Function> predicate;
        static void Init(v8::Handle<v8::Object> exports);
        static v8::Handle<v8::Value> New(const v8::Arguments& args);

    public:
        HyperDexClient(const char* host, uint16_t port);
        ~HyperDexClient() throw ();

    public:
        static v8::Handle<v8::Value> asInt(const v8::Arguments& args);
        static v8::Handle<v8::Value> asFloat(const v8::Arguments& args);
        static v8::Handle<v8::Value> asList(const v8::Arguments& args);
        static v8::Handle<v8::Value> asSet(const v8::Arguments& args);
        static v8::Handle<v8::Value> asMap(const v8::Arguments& args);
        static v8::Handle<v8::Value> loop(const v8::Arguments& args);
        static v8::Handle<v8::Value> loop_error(const v8::Arguments& args);

        static v8::Handle<v8::Value> Equals(const v8::Arguments& args);
        static v8::Handle<v8::Value> LessThan(const v8::Arguments& args);
        static v8::Handle<v8::Value> LessEqual(const v8::Arguments& args);
        static v8::Handle<v8::Value> GreaterEqual(const v8::Arguments& args);
        static v8::Handle<v8::Value> GreaterThan(const v8::Arguments& args);
        static v8::Handle<v8::Value> Regex(const v8::Arguments& args);
        static v8::Handle<v8::Value> LengthEquals(const v8::Arguments& args);
        static v8::Handle<v8::Value> LengthLessEqual(const v8::Arguments& args);
        static v8::Handle<v8::Value> LengthGreaterEqual(const v8::Arguments& args);
        static v8::Handle<v8::Value> Contains(const v8::Arguments& args);

#include "client.declarations.cc"

    public:
        hyperdex_client* client() { return m_cl; }
        void add(int64_t reqid, e::intrusive_ptr<Operation> op);
        void loop(int timeout);
        void poll_start();
        void poll_stop();

    public:
        static void poll_callback(uv_poll_t* watcher, int status, int revents);
        static void poll_cleanup(uv_handle_t* handle);

    private:
        hyperdex_client* m_cl;
        std::map<int64_t, e::intrusive_ptr<Operation> > m_ops;
        v8::Persistent<v8::Object> m_callback;
        uv_poll_t* m_poll;
        bool m_poll_active;
};

typedef int (*elem_string_fptr)(void*, const char*, size_t, enum hyperdex_ds_returncode*);
typedef int (*elem_int_fptr)(void*, int64_t, enum hyperdex_ds_returncode*);
typedef int (*elem_float_fptr)(void*, double, enum hyperdex_ds_returncode*);

class Operation
{
    public:
        Operation(v8::Handle<v8::Object>& c1, HyperDexClient* c2);
        ~Operation() throw ();

    public:
        typedef bool (Operation::*type_converter)(v8::Handle<v8::Value>&, const char**, size_t*, hyperdatatype*);

        bool set_callback(v8::Handle<v8::Function>& func);
        bool set_callback(v8::Handle<v8::Function>& func,
                          v8::Handle<v8::Function>& done);
        bool convert_error(v8::Handle<v8::Value>& _value,
                           const char** value, size_t* value_sz,
                           hyperdatatype* datatype);
        bool convert_int(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype);
        bool convert_float(v8::Handle<v8::Value>& _value,
                           const char** value, size_t* value_sz,
                           hyperdatatype* datatype);
        bool convert_elem(v8::Handle<v8::Value>& value, void* x,
                          elem_string_fptr f_string,
                          elem_int_fptr f_int,
                          elem_float_fptr f_float);
        bool convert_list(v8::Handle<v8::Value>& _value,
                          const char** value, size_t* value_sz,
                          hyperdatatype* datatype);
        bool convert_set(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype);
        bool convert_map(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype);
        bool convert_type(v8::Handle<v8::Value>& _value,
                          const char** value, size_t* value_sz,
                          hyperdatatype* datatype,
                          bool make_callback_on_error,
                          bool may_default_to_json);
        bool convert_cstring(v8::Handle<v8::Value>& _cstring,
                             const char** cstring);

        bool convert_spacename(v8::Handle<v8::Value>& _spacename,
                               const char** spacename);
        bool convert_key(v8::Handle<v8::Value>& _key,
                         const char** key, size_t* key_sz);
        bool convert_attributes(v8::Handle<v8::Value>& _attributes,
                                const hyperdex_client_attribute** attrs,
                                size_t* attrs_sz);
        bool convert_document_predicates(const std::string& s,
                                         v8::Handle<v8::Object>& v,
                                         std::vector<hyperdex_client_attribute_check>* checks);
        bool convert_predicates(v8::Handle<v8::Value>& _predicates,
                                const hyperdex_client_attribute_check** checks,
                                size_t* checks_sz);
        bool convert_mapattributes(v8::Handle<v8::Value>& _predicates,
                                   const hyperdex_client_map_attribute** checks,
                                   size_t* checks_sz);
        bool convert_attributenames(v8::Handle<v8::Value>& _attributenames,
                                    const char*** attributenames,
                                    size_t* attributenames_sz);
        bool convert_sortby(v8::Handle<v8::Value>& _sortby,
                            const char** sortby);
        bool convert_limit(v8::Handle<v8::Value>& _limit,
                           uint64_t* limit);
        bool convert_maxmin(v8::Handle<v8::Value>& _maxmin,
                            int* maximize);

        // 20 Sept 2015 - William Whitacre
        // Patch against 1.8.1: Enable macaroon authorization.

        // Convert a v8 array of strings to a character pointer array.
        bool convert_auth_context(v8::Handle<v8::Value> &_value,
                                  const char ***p_auth, size_t *p_auth_sz);

        // Set and clear the auth context using the hyperdex_client API.
        bool set_auth_context(v8::Handle<v8::Value> &_value);
        bool clear_auth_context();

        bool build_string(const char* value, size_t value_sz,
                          v8::Local<v8::Value>& retval,
                          v8::Local<v8::Value>& error);
        bool build_int(const char* value, size_t value_sz,
                       v8::Local<v8::Value>& retval,
                       v8::Local<v8::Value>& error);
        bool build_float(const char* value, size_t value_sz,
                         v8::Local<v8::Value>& retval,
                         v8::Local<v8::Value>& error);
        bool build_document(const char* value, size_t value_sz,
                            v8::Local<v8::Value>& retval,
                            v8::Local<v8::Value>& error);
        bool build_list_string(const char* value, size_t value_sz,
                               v8::Local<v8::Value>& retval,
                               v8::Local<v8::Value>& error);
        bool build_list_int(const char* value, size_t value_sz,
                            v8::Local<v8::Value>& retval,
                            v8::Local<v8::Value>& error);
        bool build_list_float(const char* value, size_t value_sz,
                              v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error);
        bool build_set_string(const char* value, size_t value_sz,
                              v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error);
        bool build_set_int(const char* value, size_t value_sz,
                           v8::Local<v8::Value>& retval,
                           v8::Local<v8::Value>& error);
        bool build_set_float(const char* value, size_t value_sz,
                             v8::Local<v8::Value>& retval,
                             v8::Local<v8::Value>& error);
        bool build_map_string_string(const char* value, size_t value_sz,
                                     v8::Local<v8::Value>& retval,
                                     v8::Local<v8::Value>& error);
        bool build_map_string_int(const char* value, size_t value_sz,
                                  v8::Local<v8::Value>& retval,
                                  v8::Local<v8::Value>& error);
        bool build_map_string_float(const char* value, size_t value_sz,
                                    v8::Local<v8::Value>& retval,
                                    v8::Local<v8::Value>& error);
        bool build_map_int_string(const char* value, size_t value_sz,
                                  v8::Local<v8::Value>& retval,
                                  v8::Local<v8::Value>& error);
        bool build_map_int_int(const char* value, size_t value_sz,
                               v8::Local<v8::Value>& retval,
                               v8::Local<v8::Value>& error);
        bool build_map_int_float(const char* value, size_t value_sz,
                                 v8::Local<v8::Value>& retval,
                                 v8::Local<v8::Value>& error);
        bool build_map_float_string(const char* value, size_t value_sz,
                                    v8::Local<v8::Value>& retval,
                                    v8::Local<v8::Value>& error);
        bool build_map_float_int(const char* value, size_t value_sz,
                                 v8::Local<v8::Value>& retval,
                                 v8::Local<v8::Value>& error);
        bool build_map_float_float(const char* value, size_t value_sz,
                                   v8::Local<v8::Value>& retval,
                                   v8::Local<v8::Value>& error);
        bool build_attribute(const hyperdex_client_attribute* attr,
                             v8::Local<v8::Value>& retval,
                             v8::Local<v8::Value>& error);
        void build_attributes(v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error);

        void encode_asynccall_status();
        void encode_asynccall_status_attributes();
        void encode_asynccall_status_count();
        void encode_asynccall_status_description();
        void encode_iterator_status_attributes();

    public:
        void make_callback(v8::Handle<v8::Value>& first,
                           v8::Handle<v8::Value>& second);
        void make_callback_done();
        static v8::Local<v8::Value> error_from_status(hyperdex_client* client,
                                                      hyperdex_client_returncode status);
        v8::Local<v8::Value> error_from_status();
        v8::Local<v8::Value> error_message(const char* msg);
        v8::Local<v8::Value> error_out_of_memory();
        void callback_error(v8::Handle<v8::Value>& err);
        void callback_error_from_status();
        void callback_error_message(const char* msg);
        void callback_error_out_of_memory();

    public:
        HyperDexClient* client;
        int64_t reqid;
        enum hyperdex_client_returncode status;
        const struct hyperdex_client_attribute* attrs;
        size_t attrs_sz;
        const char* description;
        uint64_t count;
        bool finished;
        void (Operation::*encode_return)();
        const char **m_auth;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        friend class e::intrusive_ptr<Operation>;

    private:
        size_t m_ref;
        hyperdex_ds_arena* m_arena;
        v8::Persistent<v8::Object> m_client;
        v8::Persistent<v8::Object> m_callback;
        v8::Persistent<v8::Object> m_callback_done;
        bool m_has_callback_done;
};

class TypeHint : public node::ObjectWrap
{
    public:
        static v8::Handle<v8::Value> New(const v8::Arguments& args);

    public:
        TypeHint(hyperdatatype t, v8::Local<v8::Value>& o);
        ~TypeHint() throw ();

    public:
        Operation::type_converter get_converter();
        v8::Handle<v8::Value> value();
        hyperdatatype get_type();

    private:
        hyperdatatype m_type;
        v8::Persistent<v8::Value> m_obj;
};

class Predicate : public node::ObjectWrap
{
    public:
        static v8::Handle<v8::Value> New(const v8::Arguments& args);

    public:
        Predicate(hyperpredicate p, v8::Local<v8::Value>& v);
        ~Predicate() throw ();

    public:
        v8::Handle<v8::Value> value();
        hyperpredicate get_pred();

    private:
        hyperpredicate m_pred;
        v8::Persistent<v8::Value> m_obj;
};

v8::Persistent<v8::Function> HyperDexClient::ctor;
v8::Persistent<v8::Function> HyperDexClient::global_err;
v8::Persistent<v8::Function> HyperDexClient::type_hint;
v8::Persistent<v8::Function> HyperDexClient::predicate;

void
HyperDexClient :: Init(v8::Handle<v8::Object> target)
{
    v8::Local<v8::FunctionTemplate> tpl
        = v8::FunctionTemplate::New(HyperDexClient::New);
    tpl->SetClassName(v8::String::NewSymbol("Client"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "asInt", HyperDexClient::asInt);
    NODE_SET_PROTOTYPE_METHOD(tpl, "asInt", HyperDexClient::asInt);
    NODE_SET_PROTOTYPE_METHOD(tpl, "asFloat", HyperDexClient::asFloat);
    NODE_SET_PROTOTYPE_METHOD(tpl, "asList", HyperDexClient::asList);
    NODE_SET_PROTOTYPE_METHOD(tpl, "asSet", HyperDexClient::asSet);
    NODE_SET_PROTOTYPE_METHOD(tpl, "asMap", HyperDexClient::asMap);

    NODE_SET_PROTOTYPE_METHOD(tpl, "Equals", HyperDexClient::Equals);
    NODE_SET_PROTOTYPE_METHOD(tpl, "LessThan", HyperDexClient::LessThan);
    NODE_SET_PROTOTYPE_METHOD(tpl, "LessEqual", HyperDexClient::LessEqual);
    NODE_SET_PROTOTYPE_METHOD(tpl, "GreaterEqual", HyperDexClient::GreaterEqual);
    NODE_SET_PROTOTYPE_METHOD(tpl, "GreaterThan", HyperDexClient::GreaterThan);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Regex", HyperDexClient::Regex);
    NODE_SET_PROTOTYPE_METHOD(tpl, "LengthEquals", HyperDexClient::LengthEquals);
    NODE_SET_PROTOTYPE_METHOD(tpl, "LengthLessEqual", HyperDexClient::LengthLessEqual);
    NODE_SET_PROTOTYPE_METHOD(tpl, "LengthGreaterEqual", HyperDexClient::LengthGreaterEqual);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Contains", HyperDexClient::Contains);

    NODE_SET_PROTOTYPE_METHOD(tpl, "loop", HyperDexClient::loop);
#include "client.prototypes.cc"

    ctor = v8::Persistent<v8::Function>::New(tpl->GetFunction());
    target->Set(v8::String::NewSymbol("Client"), ctor);

    v8::Local<v8::FunctionTemplate> tpl_err
        = v8::FunctionTemplate::New(HyperDexClient::loop_error);
    global_err = v8::Persistent<v8::Function>::New(tpl_err->GetFunction());

    v8::Local<v8::FunctionTemplate> tpl_type
        = v8::FunctionTemplate::New(TypeHint::New);
    tpl_type->SetClassName(v8::String::NewSymbol("TypeHint"));
    tpl_type->InstanceTemplate()->SetInternalFieldCount(1);
    type_hint = v8::Persistent<v8::Function>::New(tpl_type->GetFunction());

    v8::Local<v8::FunctionTemplate> tpl_pred
        = v8::FunctionTemplate::New(Predicate::New);
    tpl_pred->SetClassName(v8::String::NewSymbol("Predicate"));
    tpl_pred->InstanceTemplate()->SetInternalFieldCount(2);
    predicate = v8::Persistent<v8::Function>::New(tpl_pred->GetFunction());
}

v8::Handle<v8::Value>
HyperDexClient :: New(const v8::Arguments& args)
{
    v8::HandleScope scope;

    if (!args.IsConstructCall())
    {
        return v8::ThrowException(v8::String::New("client must be constructed with \"new\""));
    }

    if (args.Length() != 2)
    {
        return v8::ThrowException(v8::String::New("constructor requires (host, port) arguments"));
    }

    if (args[0].IsEmpty() || !args[0]->IsString())
    {
        return v8::ThrowException(v8::String::New("constructor requires that the \"host\" argument be a string"));
    }

    if (args[1].IsEmpty() || !args[1]->IsNumber())
    {
        return v8::ThrowException(v8::String::New("constructor requires that the \"port\" argument be a number"));
    }

    v8::Local<v8::String> _host = args[0].As<v8::String>();
    v8::Local<v8::Integer> _port = args[1].As<v8::Integer>();
    v8::String::AsciiValue s(_host);

    const char* host = *s;
    uint16_t port = _port->Value();

    HyperDexClient* c = new HyperDexClient(host, port);
    c->Wrap(args.This());
    return scope.Close(args.This());
}

HyperDexClient :: HyperDexClient(const char* host, uint16_t port)
    : m_cl(hyperdex_client_create(host, port))
    , m_poll(NULL)
    , m_poll_active(false)
{
    if (m_cl)
    {
        m_poll = new uv_poll_t;
        uv_poll_init_socket(uv_default_loop(), m_poll, hyperdex_client_poll_fd(m_cl));
        m_poll->data = this;
    }

    v8::Local<v8::Function> func(v8::Local<v8::Function>::New(global_err));
    v8::Local<v8::Object> cbobj = v8::Object::New();
    cbobj->Set(v8::String::NewSymbol("callback"), func);
    m_callback = v8::Persistent<v8::Object>::New(cbobj);
}

HyperDexClient :: ~HyperDexClient() throw ()
{
    if (m_cl)
    {
        hyperdex_client_destroy(m_cl);
    }

    if (m_poll)
    {
        m_poll->data = NULL;
        poll_stop();
        uv_close(reinterpret_cast<uv_handle_t*>(m_poll), poll_cleanup);
    }

    if (!m_callback.IsEmpty())
    {
        m_callback.Dispose();
        m_callback.Clear();
    }
}

void
HyperDexClient :: add(int64_t reqid, e::intrusive_ptr<Operation> op)
{
    m_ops[reqid] = op;
    poll_start();
}

void
HyperDexClient :: loop(int timeout)
{
    enum hyperdex_client_returncode rc;
    int64_t ret = hyperdex_client_loop(m_cl, timeout, &rc);

    if (ret < 0 && timeout == 0 && rc == HYPERDEX_CLIENT_TIMEOUT)
    {
        return;
    }

    if (ret < 0)
    {
        v8::Local<v8::Object> obj = v8::Local<v8::Object>::New(m_callback);
        v8::Local<v8::Function> callback
            = obj->Get(v8::String::NewSymbol("callback")).As<v8::Function>();
        v8::Local<v8::Value> err = Operation::error_from_status(m_cl, rc);
        v8::Handle<v8::Value> argv[] = { err };
        node::MakeCallback(v8::Context::GetCurrent()->Global(), callback, 1, argv);
        return;
    }

    std::map<int64_t, e::intrusive_ptr<Operation> >::iterator it;
    it = m_ops.find(ret);
    assert(it != m_ops.end());
    (*it->second.*it->second->encode_return)();

    if (it->second->finished)
    {
        m_ops.erase(it);
    }

    if (m_ops.empty())
    {
        poll_stop();
    }
}

void
HyperDexClient :: poll_start()
{
    if (m_poll_active || !m_poll)
    {
        return;
    }

    m_poll_active = true;
    uv_poll_start(m_poll, UV_READABLE, poll_callback);
}

void
HyperDexClient :: poll_stop()
{
    if (!m_poll_active || !m_poll)
    {
        return;
    }

    uv_poll_stop(m_poll);
    m_poll_active = false;
}

void
HyperDexClient :: poll_callback(uv_poll_t* watcher, int status, int revents)
{
    HyperDexClient* cl = reinterpret_cast<HyperDexClient*>(watcher->data);

    if (cl)
    {
        cl->loop(0);
    }
}

void
HyperDexClient :: poll_cleanup(uv_handle_t* handle)
{
    uv_poll_t* poll = reinterpret_cast<uv_poll_t*>(handle);

    if (handle)
    {
        delete poll;
    }
}

v8::Handle<v8::Value>
HyperDexClient :: loop(const v8::Arguments& args)
{
    v8::HandleScope scope;
    int timeout = -1;

    if (args.Length() > 0 && args[0]->IsNumber())
    {
        timeout = args[0]->IntegerValue();
    }

    HyperDexClient* client = node::ObjectWrap::Unwrap<HyperDexClient>(args.This());
    client->loop(timeout);
    return scope.Close(v8::Undefined());
}

v8::Handle<v8::Value>
HyperDexClient :: loop_error(const v8::Arguments& args)
{
    v8::HandleScope scope;

    if (args.Length() > 0)
    {
        v8::ThrowException(args[0]);
    }

    return scope.Close(v8::Undefined());
}

v8::Handle<v8::Value>
TypeHint :: New(const v8::Arguments& args)
{
    v8::HandleScope scope;

    if (!args.IsConstructCall())
    {
        return v8::ThrowException(v8::String::New("client must be constructed with \"new\""));
    }

    if (args.Length() != 2)
    {
        return v8::ThrowException(v8::String::New("type hint requires (type, obj) arguments"));
    }

    if (args[0].IsEmpty() || !args[0]->IsString())
    {
        return v8::ThrowException(v8::String::New("type hint requires that the \"type\" argument be a string"));
    }

    if (args[1].IsEmpty())
    {
        return v8::ThrowException(v8::String::New("type hint requires something to cast"));
    }

    v8::Local<v8::String> _type = args[0].As<v8::String>();
    v8::String::AsciiValue s(_type);
    hyperdatatype t = HYPERDATATYPE_GARBAGE;

    if (strcmp("int", *s) == 0 ||
        strcmp("int64", *s) == 0)
    {
        t = HYPERDATATYPE_INT64;
    }
    else if (strcmp("float", *s) == 0)
    {
        t = HYPERDATATYPE_FLOAT;
    }
    else if (strcmp("list", *s) == 0)
    {
        t = HYPERDATATYPE_LIST_GENERIC;
    }
    else if (strcmp("set", *s) == 0)
    {
        t = HYPERDATATYPE_SET_GENERIC;
    }
    else if (strcmp("map", *s) == 0)
    {
        t = HYPERDATATYPE_MAP_GENERIC;
    }
    else
    {
        return v8::ThrowException(v8::String::New("cannot cast unknown type"));
    }

    v8::Local<v8::Value> obj = v8::Local<v8::Value>::New(args[1]);
    TypeHint* th = new TypeHint(t, obj);
    th->Wrap(args.This());
    return scope.Close(args.This());
}

TypeHint :: TypeHint(hyperdatatype t, v8::Local<v8::Value>& o)
    : m_type(t)
    , m_obj()
{
    if (!o.IsEmpty())
    {
        m_obj = v8::Persistent<v8::Value>::New(o);
    }
}

TypeHint :: ~TypeHint() throw ()
{
    if (!m_obj.IsEmpty())
    {
        m_obj.Dispose();
        m_obj.Clear();
    }
}

#define HYPERDEX_TYPE_CAST(X, Y) \
    v8::Handle<v8::Value> \
    HyperDexClient :: X(const v8::Arguments& args) \
    { \
        v8::Local<v8::Function> th = v8::Local<v8::Function>::New(type_hint); \
        v8::Local<v8::Value> t(v8::String::New(Y)); \
        v8::Handle<v8::Value> argv[] = {t, args[0]}; \
        return th->NewInstance(2, argv); \
    }

HYPERDEX_TYPE_CAST(asInt, "int");
HYPERDEX_TYPE_CAST(asFloat, "float");
HYPERDEX_TYPE_CAST(asList, "list");
HYPERDEX_TYPE_CAST(asSet, "set");
HYPERDEX_TYPE_CAST(asMap, "map");

Operation::type_converter
TypeHint :: get_converter()
{
    switch (m_type)
    {
        case HYPERDATATYPE_INT64:
            return &Operation::convert_int;
        case HYPERDATATYPE_FLOAT:
            return &Operation::convert_float;
        case HYPERDATATYPE_LIST_GENERIC:
            return &Operation::convert_list;
        case HYPERDATATYPE_SET_GENERIC:
            return &Operation::convert_set;
        case HYPERDATATYPE_MAP_GENERIC:
            return &Operation::convert_map;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_STRING:
        case HYPERDATATYPE_LIST_STRING:
        case HYPERDATATYPE_LIST_INT64:
        case HYPERDATATYPE_LIST_FLOAT:
        case HYPERDATATYPE_SET_STRING:
        case HYPERDATATYPE_SET_INT64:
        case HYPERDATATYPE_SET_FLOAT:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_STRING_STRING:
        case HYPERDATATYPE_MAP_STRING_INT64:
        case HYPERDATATYPE_MAP_STRING_FLOAT:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_STRING:
        case HYPERDATATYPE_MAP_INT64_INT64:
        case HYPERDATATYPE_MAP_INT64_FLOAT:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_STRING:
        case HYPERDATATYPE_MAP_FLOAT_INT64:
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
        case HYPERDATATYPE_GARBAGE:
        default:
            return &Operation::convert_error;
    }
}

v8::Handle<v8::Value>
TypeHint :: value()
{
    v8::HandleScope scope;
    return scope.Close(v8::Local<v8::Value>::New(m_obj));
}

hyperdatatype
TypeHint :: get_type()
{
    return m_type;
}

v8::Handle<v8::Value>
Predicate :: New(const v8::Arguments& args)
{
    v8::HandleScope scope;

    if (!args.IsConstructCall())
    {
        return v8::ThrowException(v8::String::New("client must be constructed with \"new\""));
    }

    if (args.Length() != 2)
    {
        return v8::ThrowException(v8::String::New("predicate requires (predicate, obj) arguments"));
    }

    if (args[0].IsEmpty() || !args[0]->IsNumber())
    {
        return v8::ThrowException(v8::String::New("predicate requires that the \"predicate\" argument be a number"));
    }

    if (args[1].IsEmpty())
    {
        return v8::ThrowException(v8::String::New("predicate requires something to cast"));
    }

    int _p = args[0].As<v8::Number>()->Value();
    hyperpredicate p = static_cast<hyperpredicate>(_p);

    switch (p)
    {
        case HYPERPREDICATE_FAIL:
        case HYPERPREDICATE_EQUALS:
        case HYPERPREDICATE_LESS_THAN:
        case HYPERPREDICATE_LESS_EQUAL:
        case HYPERPREDICATE_GREATER_EQUAL:
        case HYPERPREDICATE_GREATER_THAN:
        case HYPERPREDICATE_CONTAINS_LESS_THAN:
        case HYPERPREDICATE_REGEX:
        case HYPERPREDICATE_LENGTH_EQUALS:
        case HYPERPREDICATE_LENGTH_LESS_EQUAL:
        case HYPERPREDICATE_LENGTH_GREATER_EQUAL:
        case HYPERPREDICATE_CONTAINS:
            break;
        default:
            abort();
    }

    v8::Local<v8::Value> obj = v8::Local<v8::Value>::New(args[1]);
    Predicate* pred = new Predicate(p, obj);
    pred->Wrap(args.This());
    return scope.Close(args.This());
}

Predicate :: Predicate(hyperpredicate p, v8::Local<v8::Value>& o)
    : m_pred(p)
    , m_obj()
{
    if (!o.IsEmpty())
    {
        m_obj = v8::Persistent<v8::Value>::New(o);
    }
}

Predicate :: ~Predicate() throw ()
{
    if (!m_obj.IsEmpty())
    {
        m_obj.Dispose();
        m_obj.Clear();
    }
}

#define HYPERDEX_PRED_CAST(X, Y) \
    v8::Handle<v8::Value> \
    HyperDexClient :: X(const v8::Arguments& args) \
    { \
        v8::Local<v8::Function> pred = v8::Local<v8::Function>::New(predicate); \
        v8::Local<v8::Value> p(v8::Integer::New(Y)); \
        v8::Handle<v8::Value> argv[] = {p, args[0]}; \
        return pred->NewInstance(2, argv); \
    }

HYPERDEX_PRED_CAST(Equals,              HYPERPREDICATE_EQUALS)
HYPERDEX_PRED_CAST(LessThan,            HYPERPREDICATE_LESS_THAN)
HYPERDEX_PRED_CAST(LessEqual,           HYPERPREDICATE_LESS_EQUAL)
HYPERDEX_PRED_CAST(GreaterEqual,        HYPERPREDICATE_GREATER_EQUAL)
HYPERDEX_PRED_CAST(GreaterThan,         HYPERPREDICATE_GREATER_THAN)
HYPERDEX_PRED_CAST(Regex,               HYPERPREDICATE_REGEX)
HYPERDEX_PRED_CAST(LengthEquals,        HYPERPREDICATE_LENGTH_EQUALS)
HYPERDEX_PRED_CAST(LengthLessEqual,     HYPERPREDICATE_LENGTH_LESS_EQUAL)
HYPERDEX_PRED_CAST(LengthGreaterEqual,  HYPERPREDICATE_LENGTH_GREATER_EQUAL)
HYPERDEX_PRED_CAST(Contains,            HYPERPREDICATE_CONTAINS)

v8::Handle<v8::Value>
Predicate :: value()
{
    v8::HandleScope scope;
    return scope.Close(v8::Local<v8::Value>::New(m_obj));
}

hyperpredicate
Predicate :: get_pred()
{
    return m_pred;
}

#include "client.definitions.cc"

Operation :: Operation(v8::Handle<v8::Object>& c1, HyperDexClient* c2)
    : client(c2)
    , reqid(-1)
    , status(HYPERDEX_CLIENT_GARBAGE)
    , attrs(NULL)
    , attrs_sz(0)
    , description(NULL)
    , count(0)
    , finished(false)
    , encode_return()
    , m_auth(NULL)
    , m_ref(0)
    , m_arena(hyperdex_ds_arena_create())
    , m_client()
    , m_callback()
    , m_callback_done()
    , m_has_callback_done(false)
{
    m_client = v8::Persistent<v8::Object>::New(c1);
    v8::Local<v8::Object> cbobj1 = v8::Object::New();
    m_callback = v8::Persistent<v8::Object>::New(cbobj1);
    v8::Local<v8::Object> cbobj2 = v8::Object::New();
    m_callback_done = v8::Persistent<v8::Object>::New(cbobj2);
}

Operation :: ~Operation() throw ()
{
    if (m_arena)
    {
        hyperdex_ds_arena_destroy(m_arena);
    }

    if (attrs)
    {
        hyperdex_client_destroy_attrs(attrs, attrs_sz);
    }

    if (m_auth)
    {
        free((void *)m_auth);
    }

    if (description)
    {
        free((void*)description);
    }

    if (!m_client.IsEmpty())
    {
        m_client.Dispose();
        m_client.Clear();
    }

    if (!m_callback.IsEmpty())
    {
        m_callback.Dispose();
        m_callback.Clear();
    }
}

bool
Operation :: set_callback(v8::Handle<v8::Function>& func)
{
    v8::Local<v8::Object>::New(m_callback)->Set(v8::String::NewSymbol("callback"), func);
    m_has_callback_done = false;
    return true;
}

bool
Operation :: set_callback(v8::Handle<v8::Function>& func, v8::Handle<v8::Function>& done)
{
    v8::Local<v8::Object>::New(m_callback)->Set(v8::String::NewSymbol("callback"), func);
    v8::Local<v8::Object>::New(m_callback_done)->Set(v8::String::NewSymbol("callback"), done);
    m_has_callback_done = true;
    return true;
}

bool
Operation :: convert_error(v8::Handle<v8::Value>& _value,
                           const char** value, size_t* value_sz,
                           hyperdatatype* datatype)
{
    this->callback_error_message("cannot convert to HyperDex type");
    return false;
}

bool
Operation :: convert_int(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype)
{
    if (_value->IsNumber())
    {
        double _num = _value.As<v8::Number>()->Value();
        int64_t num = _num;
        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_int(m_arena, num, &error, value, value_sz) < 0)
        {
            this->callback_error_out_of_memory();
            return false;
        }

        *datatype = HYPERDATATYPE_INT64;
        return true;
    }
    else
    {
        this->callback_error_message("cannot convert to an int");
        return false;
    }
}

bool
Operation :: convert_float(v8::Handle<v8::Value>& _value,
                           const char** value, size_t* value_sz,
                           hyperdatatype* datatype)
{
    if (_value->IsNumber())
    {
        double num = _value.As<v8::Number>()->Value();
        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_float(m_arena, num, &error, value, value_sz) < 0)
        {
            this->callback_error_out_of_memory();
            return false;
        }

        *datatype = HYPERDATATYPE_FLOAT;
        return true;
    }
    else
    {
        this->callback_error_message("cannot convert to a float");
        return false;
    }
}

#define HDJS_HANDLE_ELEM_ERROR(X, TYPE) \
    switch (X) \
    { \
        case HYPERDEX_DS_NOMEM: \
            this->callback_error_out_of_memory(); \
            return false; \
        case HYPERDEX_DS_MIXED_TYPES: \
            this->callback_error_message("Cannot add " TYPE " to a heterogenous container"); \
            return false; \
        case HYPERDEX_DS_SUCCESS: \
        case HYPERDEX_DS_STRING_TOO_LONG: \
        case HYPERDEX_DS_WRONG_STATE: \
        default: \
            this->callback_error_message("Cannot convert " TYPE " to a HyperDex type"); \
            return false; \
    }

bool
Operation :: convert_elem(v8::Handle<v8::Value>& value, void* x,
                          elem_string_fptr f_string,
                          elem_int_fptr f_int,
                          elem_float_fptr f_float)
{
    if (value.IsEmpty())
    {
        this->callback_error_message("cannot convert undefined to HyperDex type");
        return false;
    }

    if (value->IsObject() &&
        value->ToObject()->GetConstructor()->StrictEquals(HyperDexClient::type_hint))
    {
        TypeHint* hint = node::ObjectWrap::Unwrap<TypeHint>(value.As<v8::Object>());
        assert(hint);

        if (hint->get_type() == HYPERDATATYPE_INT64)
        {
            double _num = hint->value().As<v8::Number>()->Value();
            int64_t num = _num;
            hyperdex_ds_returncode error;

            if (f_int(x, num, &error) < 0)
            {
                HDJS_HANDLE_ELEM_ERROR(error, "int");
            }

            return true;
        }
        else if (hint->get_type() == HYPERDATATYPE_FLOAT)
        {
            double num = hint->value().As<v8::Number>()->Value();
            hyperdex_ds_returncode error;

            if (f_float(x, num, &error) < 0)
            {
                HDJS_HANDLE_ELEM_ERROR(error, "float");
            }

            return true;
        }
        else
        {
            this->callback_error_message("cannot convert to HyperDex type");
            return false;
        }
    }

    if (value->IsNumber())
    {
        double _num = value.As<v8::Number>()->Value();
        int64_t num = _num;
        hyperdex_ds_returncode error;

        if (f_int(x, num, &error) < 0)
        {
            HDJS_HANDLE_ELEM_ERROR(error, "int");
        }

        return true;
    }

    if (node::Buffer::HasInstance(value->ToObject()))
    {
        size_t sz = node::Buffer::Length(value->ToObject());
        char* data = node::Buffer::Data(value->ToObject());
        hyperdex_ds_returncode error;

        if (f_string(x, data, sz, &error) < 0)
        {
            HDJS_HANDLE_ELEM_ERROR(error, "string");
        }

        return true;
    }

    if (value->IsString())
    {
        hyperdex_ds_returncode error;
        v8::String::Utf8Value s(value);

        if (s.length() == 0 && *s == NULL)
        {
            this->callback_error_message("cannot convert object to string");
            return false;
        }

        if (f_string(x, *s, s.length(), &error) < 0)
        {
            HDJS_HANDLE_ELEM_ERROR(error, "string");
        }

        return true;
    }

    this->callback_error_message("cannot convert to HyperDex type");
    return false;
}

bool
Operation :: convert_list(v8::Handle<v8::Value>& _value,
                          const char** value, size_t* value_sz,
                          hyperdatatype* datatype)
{
    if (!_value->IsArray())
    {
        this->callback_error_message("lists must be specified as arrays");
        return false;
    }

    struct hyperdex_ds_list* list;
    enum hyperdex_ds_returncode error;
    list = hyperdex_ds_allocate_list(m_arena);

    if (!list)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    v8::Local<v8::Array> arr = v8::Local<v8::Array>::New(_value.As<v8::Array>());

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        v8::Local<v8::Value> entry = arr->Get(i);

        if (!this->convert_elem(entry, list,
                (elem_string_fptr) hyperdex_ds_list_append_string,
                (elem_int_fptr) hyperdex_ds_list_append_int,
                (elem_float_fptr) hyperdex_ds_list_append_float))
        {
            return false;
        }
    }

    if (hyperdex_ds_list_finalize(list, &error, value, value_sz, datatype) < 0)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    return true;
}

bool
Operation :: convert_set(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype)
{
    if (!_value->IsArray())
    {
        this->callback_error_message("sets must be specified as arrays");
        return false;
    }

    struct hyperdex_ds_set* set;
    enum hyperdex_ds_returncode error;
    set = hyperdex_ds_allocate_set(m_arena);

    if (!set)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    v8::Local<v8::Array> arr = v8::Local<v8::Array>::New(_value.As<v8::Array>());

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        v8::Local<v8::Value> entry = arr->Get(i);

        if (!this->convert_elem(entry, set,
                (elem_string_fptr) hyperdex_ds_set_insert_string,
                (elem_int_fptr) hyperdex_ds_set_insert_int,
                (elem_float_fptr) hyperdex_ds_set_insert_float))
        {
            return false;
        }
    }

    if (hyperdex_ds_set_finalize(set, &error, value, value_sz, datatype) < 0)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    return true;
}

bool
Operation :: convert_map(v8::Handle<v8::Value>& _value,
                         const char** value, size_t* value_sz,
                         hyperdatatype* datatype)
{
    if (!_value->IsArray())
    {
        this->callback_error_message("maps must be specified as arrays");
        return false;
    }

    struct hyperdex_ds_map* map;
    enum hyperdex_ds_returncode error;
    map = hyperdex_ds_allocate_map(m_arena);

    if (!map)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    v8::Local<v8::Array> arr = v8::Local<v8::Array>::New(_value.As<v8::Array>());

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        v8::Local<v8::Value> _pair = arr->Get(i);

        if (!_pair->IsArray())
        {
            this->callback_error_message("map pairs must be specified as arrays");
            return false;
        }

        v8::Local<v8::Array> pair = _pair.As<v8::Array>();

        if (pair->Length() != 2)
        {
            this->callback_error_message("map pairs must be of length 2");
            return false;
        }

        v8::Local<v8::Value> key = pair->Get(0);
        v8::Local<v8::Value> val = pair->Get(1);

        if (!this->convert_elem(key, map,
                (elem_string_fptr) hyperdex_ds_map_insert_key_string,
                (elem_int_fptr) hyperdex_ds_map_insert_key_int,
                (elem_float_fptr) hyperdex_ds_map_insert_key_float) ||
            !this->convert_elem(val, map,
                (elem_string_fptr) hyperdex_ds_map_insert_val_string,
                (elem_int_fptr) hyperdex_ds_map_insert_val_int,
                (elem_float_fptr) hyperdex_ds_map_insert_val_float))
        {
            return false;
        }
    }

    if (hyperdex_ds_map_finalize(map, &error, value, value_sz, datatype) < 0)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    return true;
}

bool
Operation :: convert_cstring(v8::Handle<v8::Value>& _cstring,
                             const char** cstring)
{
    v8::String::Utf8Value s(_cstring);

    if (s.length() == 0 && *s == NULL)
    {
        this->callback_error_message("cannot convert object to string");
        return false;
    }

    hyperdex_ds_returncode error;
    size_t sz;

    if (hyperdex_ds_copy_string(m_arena, *s, s.length() + 1, &error, cstring, &sz) < 0)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    return true;
}

bool
Operation :: convert_type(v8::Handle<v8::Value>& _value,
                          const char** value, size_t* value_sz,
                          hyperdatatype* datatype,
                          bool make_callback_on_error,
                          bool may_default_to_json)
{
    if (_value.IsEmpty())
    {
        if (make_callback_on_error)
        {
            this->callback_error_message("cannot convert undefined to HyperDex type");
        }

        return false;
    }

    if (_value->IsObject() &&
        _value->ToObject()->GetConstructor()->StrictEquals(HyperDexClient::type_hint))
    {
        TypeHint* hint = node::ObjectWrap::Unwrap<TypeHint>(_value.As<v8::Object>());
        assert(hint);
        Operation::type_converter conv = hint->get_converter();
        v8::Local<v8::Value> inner = v8::Local<v8::Value>::New(hint->value());
        return (this->*conv)(inner, value, value_sz, datatype);
    }

    if (_value->IsNumber())
    {
        double _num = _value.As<v8::Number>()->Value();
        int64_t num = _num;
        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_int(m_arena, num, &error, value, value_sz) < 0)
        {
            if (make_callback_on_error)
            {
                this->callback_error_out_of_memory();
            }

            return false;
        }

        *datatype = HYPERDATATYPE_INT64;
        return true;
    }

    if (node::Buffer::HasInstance(_value->ToObject()))
    {
        size_t sz = node::Buffer::Length(_value->ToObject());
        char* data = node::Buffer::Data(_value->ToObject());
        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_string(m_arena, data, sz, &error, value, value_sz) < 0)
        {
            if (make_callback_on_error)
            {
                this->callback_error_out_of_memory();
            }

            return false;
        }

        *datatype = HYPERDATATYPE_STRING;
        return true;
    }

    if (_value->IsString())
    {
        v8::String::Utf8Value s(_value);

        if (s.length() == 0 && *s == NULL)
        {
            if (make_callback_on_error)
            {
                this->callback_error_message("cannot convert object to string");
            }

            return false;
        }

        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_string(m_arena, *s, s.length(), &error, value, value_sz) < 0)
        {
            if (make_callback_on_error)
            {
                this->callback_error_out_of_memory();
            }

            return false;
        }

        *datatype = HYPERDATATYPE_STRING;
        return true;
    }

    if (may_default_to_json)
    {
        v8::Local<v8::Object> global = v8::Context::GetCurrent()->Global();
        v8::Local<v8::Object> JSON = global->Get(v8::String::New("JSON"))->ToObject();
        v8::Handle<v8::Function> parse = v8::Handle<v8::Function>::Cast(JSON->Get(v8::String::New("stringify")));
        v8::Handle<v8::Value> argv[1] = { _value };
        v8::Local<v8::Value> str = v8::Local<v8::Value>::New(parse->Call(JSON, 1, argv));
        v8::String::Utf8Value s(str);

        if (s.length() == 0 && *s == NULL)
        {
            if (make_callback_on_error)
            {
                this->callback_error_message("cannot convert object to JSON");
            }

            return false;
        }

        hyperdex_ds_returncode error;

        if (hyperdex_ds_copy_string(m_arena, *s, s.length(), &error, value, value_sz) < 0)
        {
            if (make_callback_on_error)
            {
                this->callback_error_out_of_memory();
            }

            return false;
        }

        *datatype = HYPERDATATYPE_DOCUMENT;
        return true;
    }

    if (make_callback_on_error)
    {
        this->callback_error_message("cannot convert to HyperDex type");
    }

    return false;
}

bool
Operation :: convert_spacename(v8::Handle<v8::Value>& _spacename,
                               const char** spacename)
{
    return convert_cstring(_spacename, spacename);
}

bool
Operation :: convert_key(v8::Handle<v8::Value>& _key,
                         const char** key, size_t* key_sz)
{
    hyperdatatype datatype;
    return convert_type(_key, key, key_sz, &datatype, true, false);
}

bool
Operation :: convert_attributes(v8::Handle<v8::Value>& x,
                                const hyperdex_client_attribute** _attrs,
                                size_t* _attrs_sz)
{
    if (!x->IsObject())
    {
        this->callback_error_message("attributes must be an object");
        return false;
    }

    v8::Local<v8::Object> attributes = x->ToObject();
    v8::Local<v8::Array> arr = attributes->GetPropertyNames();
    size_t ret_attrs_sz = arr->Length();
    struct hyperdex_client_attribute* ret_attrs;
    ret_attrs = hyperdex_ds_allocate_attribute(m_arena, ret_attrs_sz);

    if (!ret_attrs)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    *_attrs = ret_attrs;
    *_attrs_sz = ret_attrs_sz;

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        v8::Local<v8::Value> key = arr->Get(i);
        v8::Local<v8::Value> val = attributes->Get(key);

        if (!convert_cstring(key, &ret_attrs[i].attr) ||
            !convert_type(val, &ret_attrs[i].value,
                               &ret_attrs[i].value_sz,
                               &ret_attrs[i].datatype, true, true))
        {
            return false;
        }
    }

    return true;
}

bool
Operation :: convert_document_predicates(const std::string& prefix,
                                         v8::Handle<v8::Object>& x,
                                         std::vector<hyperdex_client_attribute_check>* checks)
{
    v8::Local<v8::Array> arr = x->GetPropertyNames();

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        hyperdex_client_attribute_check check;
        v8::Local<v8::Value> key = arr->Get(i);
        v8::Local<v8::Value> val = x->Get(key);
        hyperdex_ds_returncode error;
        size_t sz;

        if (convert_type(val, &check.value,
                              &check.value_sz,
                              &check.datatype, false, false))
        {
            if (!convert_cstring(key, &check.attr))
            {
                return false;
            }

            std::string attr = prefix + check.attr;

            if (hyperdex_ds_copy_string(m_arena, attr.c_str(), attr.size() + 1, &error, &check.attr, &sz) < 0)
            {
                this->callback_error_out_of_memory();
                return false;
            }

            check.predicate = HYPERPREDICATE_EQUALS;
            checks->push_back(check);
        }
        else if (val->IsObject() &&
                 val->ToObject()->GetConstructor()->StrictEquals(HyperDexClient::predicate))
        {
            Predicate* pred = node::ObjectWrap::Unwrap<Predicate>(val.As<v8::Object>());
            v8::Local<v8::Value> v = v8::Local<v8::Value>::New(pred->value());

            if (!convert_cstring(key, &check.attr) ||
                !convert_type(v, &check.value,
                              &check.value_sz,
                              &check.datatype, true, false))
            {
                return false;
            }

            std::string attr = prefix + check.attr;

            if (hyperdex_ds_copy_string(m_arena, attr.c_str(), attr.size() + 1, &error, &check.attr, &sz) < 0)
            {
                this->callback_error_out_of_memory();
                return false;
            }

            check.predicate = pred->get_pred();
            checks->push_back(check);
        }
        else if (val->IsObject())
        {
            v8::String::Utf8Value s(key);

            if (s.length() == 0 && *s == NULL)
            {
                this->callback_error_message("cannot convert object to string");
                return false;
            }

            std::string ss(prefix);
            ss.append(*s, s.length());
            ss.append(".", 1);
            v8::Local<v8::Object> ps = val->ToObject();

            if (!convert_document_predicates(ss, ps, checks))
            {
                return false;
            }
        }
    }

    return true;
}

bool
Operation :: convert_predicates(v8::Handle<v8::Value>& x,
                                const hyperdex_client_attribute_check** _checks,
                                size_t* _checks_sz)
{
    if (!x->IsObject())
    {
        this->callback_error_message("predicates must be an object");
        return false;
    }

    v8::Local<v8::Object> predicates = x->ToObject();
    std::vector<hyperdex_client_attribute_check> checks;

    if (!convert_document_predicates("", predicates, &checks))
    {
        return false;
    }

    hyperdex_client_attribute_check* cs;
    *_checks = cs = hyperdex_ds_allocate_attribute_check(m_arena, checks.size());
    *_checks_sz = checks.size();

    if (!*_checks)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    for (size_t i = 0; i < checks.size(); ++i)
    {
        cs[i] = checks[i];
    }

    return true;
}

bool
Operation :: convert_mapattributes(v8::Handle<v8::Value>& _predicates,
                                   const hyperdex_client_map_attribute** _mapattrs,
                                   size_t* _mapattrs_sz)
{
    v8::ThrowException(v8::String::New("mapattributes not yet supported")); // XXX
    *_mapattrs = NULL;
    *_mapattrs_sz = 0;
    return false;
}

bool
Operation :: convert_attributenames(v8::Handle<v8::Value>& x,
                                    const char*** attributenames,
                                    size_t* attributenames_sz)
{
    if (!x->IsArray())
    {
        this->callback_error_message("attribute names must be an array");
        return false;
    }

    v8::Handle<v8::Array> arr = v8::Handle<v8::Array>::Cast(x);
    *attributenames_sz = arr->Length();
    const char** ret = static_cast<const char**>(hyperdex_ds_malloc(m_arena, sizeof(char*) * *attributenames_sz));

    if (!ret)
    {
        this->callback_error_out_of_memory();
        return false;
    }

    *attributenames = ret;

    for (size_t i = 0; i < arr->Length(); ++i)
    {
        v8::Local<v8::Value> key = arr->Get(i);

        if (!convert_cstring(key, &ret[i]))
        {
            return false;
        }
    }

    return true;
}

bool
Operation :: convert_sortby(v8::Handle<v8::Value>& _sortby,
                            const char** sortby)
{
    return convert_cstring(_sortby, sortby);
}

bool
Operation :: convert_limit(v8::Handle<v8::Value>& _limit,
                           uint64_t* limit)
{
    if (!_limit->IsNumber())
    {
        this->callback_error_message("limit must be numeric");
        return false;
    }

    *limit = _limit->IntegerValue();
    return true;
}

bool
Operation :: convert_maxmin(v8::Handle<v8::Value>& _maxmin,
                            int* maximize)
{
    *maximize = _maxmin->IsTrue() ? 1 : 0;
    return true;
}

// 20 Sept 2015 - William Whitacre
// Patch against 1.8.1: Enable macaroon authorization.
bool
Operation :: convert_auth_context(v8::Handle<v8::Value> &_value,
                                  const char ***p_auth, size_t *p_auth_sz)
{
    // Make sure this is an array we've been passed.
    if (!_value->IsArray())
    {
        this->callback_error_message("auth must be specified as an array");
        return false;
    }

    // Reference it as an array.
    v8::Local<v8::Array> arr =
        v8::Local<v8::Array>::New(_value.As<v8::Array>());

    // Allocate the pointer array for storing the authorization chain.
    const size_t auth_sz = arr->Length();
    const char **auth = reinterpret_cast<const char **>(malloc(auth_sz * sizeof(const char *)));

    // Loop through the array elements.
    for (size_t i = 0; i < auth_sz; ++i)
    {
        // Retrieve array element at index i.
        v8::Local<v8::Value> _M = arr->Get(i);

        // Make sure it's actually a string and use the convert_cstring member
        // function to copy the string.
        if (_M.IsEmpty() ||
            !(_M->IsString() && this->convert_cstring(_M, &auth[i])))
        {
            this->callback_error_message("auth must be an array of strings");
            return false;
        }
    }

    // Copy the resulting pointers to the return arguments.
    *p_auth_sz = auth_sz;
    *p_auth = auth;

    // Everything worked ok.
    return true;
}

bool
Operation :: set_auth_context(v8::Handle<v8::Value> &_value)
{
    // Clear the authorization context if it's already been set (wierd).
    this->clear_auth_context();
    size_t auth_sz;

    // Convert the authorization context object to an array of character
    // pointers.
    if (this->convert_auth_context(_value, &this->m_auth, &auth_sz))
    {
        // Call hyperdex client to set the authorization context.
        hyperdex_client_set_auth_context(
            this->client->client(),
            this->m_auth, auth_sz
        );
        return true;
    }
    else
    {
        // Something went wrong during conversion!
        return false;
    }
}

bool
Operation :: clear_auth_context()
{
    // Make sure the authorization context has been set.
    if (this->m_auth != NULL)
    {
        // Call hyperdex client to clear the authorization context.
        hyperdex_client_clear_auth_context(this->client->client());

        // Free the array.
        free((void *)this->m_auth);
        this->m_auth = NULL;
    }

    // this->m_auth is always null at this point, and if an array was there
    // it was freed.
    return true;
}

bool
Operation :: build_string(const char* value, size_t value_sz,
                          v8::Local<v8::Value>& retval,
                          v8::Local<v8::Value>& error)
{
    node::Buffer* buf = node::Buffer::New(value_sz);
    memmove(node::Buffer::Data(buf), value, value_sz);
    v8::Local<v8::Object> global = v8::Context::GetCurrent()->Global();
    v8::Local<v8::Function> buf_ctor
        = v8::Local<v8::Function>::Cast(global->Get(v8::String::NewSymbol("Buffer")));
    v8::Local<v8::Integer> A(v8::Integer::New(value_sz));
    v8::Local<v8::Integer> B(v8::Integer::New(0));
    v8::Handle<v8::Value> argv[3] = { buf->handle_, A, B };
    v8::Local<v8::Object> jsbuf = buf_ctor->NewInstance(3, argv);
    retval = jsbuf;
    return true;
}

bool
Operation :: build_int(const char* value, size_t value_sz,
                       v8::Local<v8::Value>& retval,
                       v8::Local<v8::Value>& error)
{
    int64_t tmp;

    if (hyperdex_ds_unpack_int(value, value_sz, &tmp) < 0)
    {
        return false;
    }

    retval = v8::Integer::New(tmp);
    return true;
}

bool
Operation :: build_float(const char* value, size_t value_sz,
                         v8::Local<v8::Value>& retval,
                         v8::Local<v8::Value>& error)
{
    double tmp;

    if (hyperdex_ds_unpack_float(value, value_sz, &tmp) < 0)
    {
        return false;
    }

    retval = v8::Number::New(tmp);
    return true;
}

bool
Operation :: build_document(const char* value, size_t value_sz,
                            v8::Local<v8::Value>& retval,
                            v8::Local<v8::Value>& error)
{
    v8::Local<v8::Object> global = v8::Context::GetCurrent()->Global();
    v8::Local<v8::Object> JSON = global->Get(v8::String::New("JSON"))->ToObject();
    v8::Handle<v8::Function> parse = v8::Handle<v8::Function>::Cast(JSON->Get(v8::String::New("parse")));
    v8::Local<v8::String> str(v8::String::New(value, value_sz));
    v8::Handle<v8::Value> argv[1] = { str };
    retval = v8::Local<v8::Value>::New(parse->Call(JSON, 1, argv));
    return true;
}

bool
Operation :: build_list_string(const char* value, size_t value_sz,
                               v8::Local<v8::Value>& retval,
                               v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_STRING, value, value_sz);
    const char* tmp_str = NULL;
    size_t tmp_str_sz = 0;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_list_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed list(string)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_STRING, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_list_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
    {
        v8::Local<v8::Value> elem;

        if (!build_string(tmp_str, tmp_str_sz, elem, error))
        {
            return false;
        }

        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_list_int(const char* value, size_t value_sz,
                            v8::Local<v8::Value>& retval,
                            v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_INT64, value, value_sz);
    int64_t tmp;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_list_int_next(&iter, &tmp)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed list(int)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_INT64, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_list_int_next(&iter, &tmp)) > 0)
    {
        v8::Local<v8::Value> elem(v8::Integer::New(tmp));
        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_list_float(const char* value, size_t value_sz,
                              v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_FLOAT, value, value_sz);
    double tmp;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_list_float_next(&iter, &tmp)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed list(float)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_FLOAT, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_list_float_next(&iter, &tmp)) > 0)
    {
        v8::Local<v8::Value> elem(v8::Number::New(tmp));
        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_set_string(const char* value, size_t value_sz,
                               v8::Local<v8::Value>& retval,
                               v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_STRING, value, value_sz);
    const char* tmp_str = NULL;
    size_t tmp_str_sz = 0;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_set_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed set(string)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_STRING, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_set_string_next(&iter, &tmp_str, &tmp_str_sz)) > 0)
    {
        v8::Local<v8::Value> elem;

        if (!build_string(tmp_str, tmp_str_sz, elem, error))
        {
            return false;
        }

        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_set_int(const char* value, size_t value_sz,
                            v8::Local<v8::Value>& retval,
                            v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_INT64, value, value_sz);
    int64_t tmp;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_set_int_next(&iter, &tmp)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed set(int)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_INT64, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_set_int_next(&iter, &tmp)) > 0)
    {
        v8::Local<v8::Value> elem(v8::Integer::New(tmp));
        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_set_float(const char* value, size_t value_sz,
                              v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_FLOAT, value, value_sz);
    double tmp;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_set_float_next(&iter, &tmp)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed set(float)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_FLOAT, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_set_float_next(&iter, &tmp)) > 0)
    {
        v8::Local<v8::Value> elem(v8::Number::New(tmp));
        arr->Set(idx, elem);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_string_string(const char* value, size_t value_sz,
                                      v8::Local<v8::Value>& retval,
                                      v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_STRING, value, value_sz);
    const char* k = NULL;
    const char* v = NULL;
    size_t k_sz = 0;
    size_t v_sz = 0;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_string_string_next(&iter, &k, &k_sz, &v, &v_sz)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(string, string)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_STRING, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_string_string_next(&iter, &k, &k_sz, &v, &v_sz)) > 0)
    {
        v8::Local<v8::Value> key;
        v8::Local<v8::Value> val;

        if (!build_string(k, k_sz, key, error) ||
            !build_string(v, v_sz, val, error))
        {
            return false;
        }

        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_string_int(const char* value, size_t value_sz,
                                   v8::Local<v8::Value>& retval,
                                   v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_INT64, value, value_sz);
    const char* k = NULL;
    size_t k_sz = 0;
    int64_t v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_string_int_next(&iter, &k, &k_sz, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(string, int)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_INT64, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_string_int_next(&iter, &k, &k_sz, &v)) > 0)
    {
        v8::Local<v8::Value> key;
        v8::Local<v8::Value> val(v8::Integer::New(v));

        if (!build_string(k, k_sz, key, error))
        {
            return false;
        }

        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_string_float(const char* value, size_t value_sz,
                                     v8::Local<v8::Value>& retval,
                                     v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_FLOAT, value, value_sz);
    const char* k = NULL;
    size_t k_sz = 0;
    double v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_string_float_next(&iter, &k, &k_sz, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(string, float)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_FLOAT, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_string_float_next(&iter, &k, &k_sz, &v)) > 0)
    {
        v8::Local<v8::Value> key;
        v8::Local<v8::Value> val(v8::Number::New(v));

        if (!build_string(k, k_sz, key, error))
        {
            return false;
        }

        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_int_string(const char* value, size_t value_sz,
                                  v8::Local<v8::Value>& retval,
                                  v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_STRING, value, value_sz);
    int64_t k;
    const char* v = NULL;
    size_t v_sz = 0;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_int_string_next(&iter, &k, &v, &v_sz)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(int, string)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_STRING, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_int_string_next(&iter, &k, &v, &v_sz)) > 0)
    {
        v8::Local<v8::Value> key(v8::Integer::New(k));
        v8::Local<v8::Value> val;

        if (!build_string(v, v_sz, val, error))
        {
            return false;
        }

        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_int_int(const char* value, size_t value_sz,
                               v8::Local<v8::Value>& retval,
                               v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_INT64, value, value_sz);
    int64_t k;
    int64_t v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_int_int_next(&iter, &k, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(int, int)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_INT64, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_int_int_next(&iter, &k, &v)) > 0)
    {
        v8::Local<v8::Value> key(v8::Integer::New(k));
        v8::Local<v8::Value> val(v8::Integer::New(v));
        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_int_float(const char* value, size_t value_sz,
                                 v8::Local<v8::Value>& retval,
                                 v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_FLOAT, value, value_sz);
    int64_t k;
    double v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_int_float_next(&iter, &k, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(int, float)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_INT64_FLOAT, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_int_float_next(&iter, &k, &v)) > 0)
    {
        v8::Local<v8::Value> key(v8::Integer::New(k));
        v8::Local<v8::Value> val(v8::Number::New(v));
        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_float_string(const char* value, size_t value_sz,
                                    v8::Local<v8::Value>& retval,
                                    v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_STRING, value, value_sz);
    double k;
    const char* v = NULL;
    size_t v_sz = 0;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_float_string_next(&iter, &k, &v, &v_sz)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(float, string)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_STRING, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_float_string_next(&iter, &k, &v, &v_sz)) > 0)
    {
        v8::Local<v8::Value> key(v8::Number::New(k));
        v8::Local<v8::Value> val;

        if (!build_string(v, v_sz, val, error))
        {
            return false;
        }

        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_float_int(const char* value, size_t value_sz,
                                 v8::Local<v8::Value>& retval,
                                 v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_INT64, value, value_sz);
    double k;
    int64_t v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_float_int_next(&iter, &k, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(float, int)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_INT64, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_float_int_next(&iter, &k, &v)) > 0)
    {
        v8::Local<v8::Value> key(v8::Number::New(k));
        v8::Local<v8::Value> val(v8::Integer::New(v));
        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_map_float_float(const char* value, size_t value_sz,
                                   v8::Local<v8::Value>& retval,
                                   v8::Local<v8::Value>& error)
{
    hyperdex_ds_iterator iter;
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_FLOAT, value, value_sz);
    double k;
    double v;
    size_t len = 0;
    int result = 0;

    while ((result = hyperdex_ds_iterate_map_float_float_next(&iter, &k, &v)) > 0)
    {
        ++len;
    }

    if (result < 0)
    {
        error = error_message("server sent malformed map(float, float)");
        return false;
    }

    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_FLOAT_FLOAT, value, value_sz);
    v8::Local<v8::Array> arr(v8::Array::New(len));
    size_t idx = 0;

    while ((result = hyperdex_ds_iterate_map_float_float_next(&iter, &k, &v)) > 0)
    {
        v8::Local<v8::Value> key(v8::Number::New(k));
        v8::Local<v8::Value> val(v8::Number::New(v));
        v8::Local<v8::Array> pair(v8::Array::New(2));
        pair->Set(0, key);
        pair->Set(1, val);
        arr->Set(idx, pair);
        ++idx;
    }

    retval = arr;
    return true;
}

bool
Operation :: build_attribute(const hyperdex_client_attribute* attr,
                             v8::Local<v8::Value>& retval,
                             v8::Local<v8::Value>& error)
{
    switch (attr->datatype)
    {
        case HYPERDATATYPE_STRING:
            return build_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_INT64:
            return build_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_FLOAT:
            return build_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_DOCUMENT:
            return build_document(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_LIST_STRING:
            return build_list_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_LIST_INT64:
            return build_list_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_LIST_FLOAT:
            return build_list_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_SET_STRING:
            return build_set_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_SET_INT64:
            return build_set_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_SET_FLOAT:
            return build_set_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_STRING_STRING:
            return build_map_string_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return build_map_string_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return build_map_string_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return build_map_int_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return build_map_int_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return build_map_int_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return build_map_float_string(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return build_map_float_int(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return build_map_float_float(attr->value, attr->value_sz, retval, error);
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
        default:
            error = this->error_message("server sent malformed attributes");
            return false;
    }
}

void
Operation :: build_attributes(v8::Local<v8::Value>& retval,
                              v8::Local<v8::Value>& error)
{
    v8::Local<v8::Object> obj(v8::Object::New());

    for (size_t i = 0; i < attrs_sz; ++i)
    {
        v8::Local<v8::Value> val;

        if (!build_attribute(attrs + i, val, error))
        {
            retval = v8::Local<v8::Value>::New(v8::Undefined());
            return;
        }

        obj->Set(v8::String::New(attrs[i].attr), val);
    }

    if (attrs)
    {
        hyperdex_client_destroy_attrs(attrs, attrs_sz);
        attrs = NULL;
        attrs_sz = 0;
    }

    retval = obj;
    error = v8::Local<v8::Value>::New(v8::Undefined());
}

void
Operation :: encode_asynccall_status()
{
    v8::Local<v8::Value> retval;
    v8::Local<v8::Value> error;

    if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        retval = v8::Local<v8::Value>::New(v8::True());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_NOTFOUND)
    {
        retval = v8::Local<v8::Value>::New(v8::Null());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_CMPFAIL)
    {
        retval = v8::Local<v8::Value>::New(v8::False());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else
    {
        retval = v8::Local<v8::Value>::New(v8::Undefined());
        error = v8::Local<v8::Value>::New(this->error_from_status());
    }

    finished = true;
    this->make_callback(error, retval);
}

void
Operation :: encode_asynccall_status_attributes()
{
    v8::Local<v8::Value> retval;
    v8::Local<v8::Value> error;

    if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        this->build_attributes(retval, error);
    }
    else if (status == HYPERDEX_CLIENT_NOTFOUND)
    {
        retval = v8::Local<v8::Value>::New(v8::Null());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_CMPFAIL)
    {
        retval = v8::Local<v8::Value>::New(v8::False());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else
    {
        retval = v8::Local<v8::Value>::New(v8::Undefined());
        error = v8::Local<v8::Value>::New(this->error_from_status());
    }

    finished = true;
    this->make_callback(error, retval);
}

void
Operation :: encode_asynccall_status_count()
{
    v8::Local<v8::Value> retval;
    v8::Local<v8::Value> error;

    if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        retval = v8::Local<v8::Value>::New(v8::Integer::New(count));
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_NOTFOUND)
    {
        retval = v8::Local<v8::Value>::New(v8::Null());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_CMPFAIL)
    {
        retval = v8::Local<v8::Value>::New(v8::False());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else
    {
        retval = v8::Local<v8::Value>::New(v8::Undefined());
        error = v8::Local<v8::Value>::New(this->error_from_status());
    }

    finished = true;
    this->make_callback(error, retval);
}

void
Operation :: encode_asynccall_status_description()
{
    v8::Local<v8::Value> retval;
    v8::Local<v8::Value> error;

    if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        retval = v8::Local<v8::Value>::New(v8::String::New(description));
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_NOTFOUND)
    {
        retval = v8::Local<v8::Value>::New(v8::Null());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_CMPFAIL)
    {
        retval = v8::Local<v8::Value>::New(v8::False());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else
    {
        retval = v8::Local<v8::Value>::New(v8::Undefined());
        error = v8::Local<v8::Value>::New(this->error_from_status());
    }

    finished = true;
    this->make_callback(error, retval);
}

void
Operation :: encode_iterator_status_attributes()
{
    v8::Local<v8::Value> retval;
    v8::Local<v8::Value> error;

    if (status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        finished = true;
        this->make_callback_done();
        return;
    }
    else if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        this->build_attributes(retval, error);
    }
    else if (status == HYPERDEX_CLIENT_NOTFOUND)
    {
        retval = v8::Local<v8::Value>::New(v8::Null());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else if (status == HYPERDEX_CLIENT_CMPFAIL)
    {
        retval = v8::Local<v8::Value>::New(v8::False());
        error = v8::Local<v8::Value>::New(v8::Undefined());
    }
    else
    {
        retval = v8::Local<v8::Value>::New(v8::Undefined());
        error = v8::Local<v8::Value>::New(this->error_from_status());
    }

    this->make_callback(error, retval);
}

void
Operation :: make_callback(v8::Handle<v8::Value>& first,
                           v8::Handle<v8::Value>& second)
{
    v8::Local<v8::Function> callback = v8::Local<v8::Object>::New(m_callback)->Get(v8::String::NewSymbol("callback")).As<v8::Function>();
    v8::Handle<v8::Value> argv[] = { first, second };
    node::MakeCallback(v8::Context::GetCurrent()->Global(), callback, 2, argv);
}

void
Operation :: make_callback_done()
{
    assert(m_has_callback_done);
    v8::Local<v8::Function> callback = v8::Local<v8::Object>::New(m_callback_done)->Get(v8::String::NewSymbol("callback")).As<v8::Function>();
    v8::Handle<v8::Value> argv[] = {};
    node::MakeCallback(v8::Context::GetCurrent()->Global(), callback, 0, argv);
}

v8::Local<v8::Value>
Operation :: error_from_status(hyperdex_client* client,
                               hyperdex_client_returncode status)
{
    v8::Local<v8::Object> obj(v8::Object::New());
    obj->Set(v8::String::New("msg"),
             v8::String::New(hyperdex_client_error_message(client)));
    obj->Set(v8::String::New("sym"),
             v8::String::New(hyperdex_client_returncode_to_string(status)));
    return obj;
}

v8::Local<v8::Value>
Operation :: error_from_status()
{
    return Operation::error_from_status(client->client(), status);
}

v8::Local<v8::Value>
Operation :: error_message(const char* msg)
{
    v8::Local<v8::Object> obj(v8::Object::New());
    obj->Set(v8::String::New("msg"), v8::String::New(msg));
    return obj;
}

v8::Local<v8::Value>
Operation :: error_out_of_memory()
{
    return error_message("out of memory");
}

void
Operation :: callback_error(v8::Handle<v8::Value>& err)
{
    v8::Local<v8::Value> X(v8::Local<v8::Value>::New(v8::Undefined()));
    make_callback(err, X);
}

void
Operation :: callback_error_from_status()
{
    v8::Local<v8::Value> err
        = v8::Local<v8::Value>::New(this->error_from_status());
    callback_error(err);
}

void
Operation :: callback_error_message(const char* msg)
{
    v8::Local<v8::Value> err
        = v8::Local<v8::Value>::New(this->error_message(msg));
    callback_error(err);
}

void
Operation :: callback_error_out_of_memory()
{
    v8::Local<v8::Value> err
        = v8::Local<v8::Value>::New(this->error_out_of_memory());
    callback_error(err);
}

void
Init(v8::Handle<v8::Object> target)
{
    HyperDexClient::Init(target);
}

} // namespace nodejs
} // namespace hyperdex

#pragma GCC visibility pop

NODE_MODULE(hyperdex_client, hyperdex::nodejs::Init)
