#define BUILDING_NODE_EXTENSION

#include <stdio.h>
#include <stdint.h>

#include <map>
#include <iostream>

#include <node.h>
#include <cvv8/convert.hpp>

#include "../hyperclient.h"

using namespace v8;

enum Node_Op
{
        GET,
        PUT,
        COND_PUT,
        DEL,
        SEARCH,
        ATOMIC_ADD,
        ATOMIC_SUB,
        ATOMIC_MUL,
        ATOMIC_DIV,
        ATOMIC_MOD,
        ATOMIC_AND,
        ATOMIC_OR,
        ATOMIC_XOR,
        STRING_PREPEND,
        STRING_APPEND,
        LIST_LPUSH,
        LIST_RPUSH,
	SET_ADD,
	SET_REMOVE,
	SET_UNION,
	SET_INTERSECT,
	MAP_ADD,
	MAP_REMOVE,
	MAP_ATOMIC_ADD,
	MAP_ATOMIC_SUB,
	MAP_ATOMIC_MUL,
	MAP_ATOMIC_DIV,
	MAP_ATOMIC_MOD,
	MAP_ATOMIC_AND,
	MAP_ATOMIC_OR,
	MAP_ATOMIC_XOR,
	MAP_STRING_PREPEND,
	MAP_STRING_APPEND
};


class HyperClient : public node::ObjectWrap {
 	public:
		static void Init();
  		static v8::Handle<v8::Value> NewInstance(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_get(const v8::Arguments& args);
		static v8::Handle<v8::Value> get(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_put(const v8::Arguments& args);
		static v8::Handle<v8::Value> put(const v8::Arguments& args);
		static v8::Handle<v8::Value> wait(const v8::Arguments& args);
		v8::Handle<v8::Value> wait();
		static v8::Handle<v8::Value> destroy(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_condput(const v8::Arguments& args);
                static v8::Handle<v8::Value> condput(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_search(const v8::Arguments& args);
                static v8::Handle<v8::Value> search(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_del(const v8::Arguments& args);
                static v8::Handle<v8::Value> del(const v8::Arguments& args);

		static v8::Handle<v8::Value> atomic_add(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_add(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_sub(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_sub(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_mul(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_mul(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_div(const v8::Arguments& args);		
		static v8::Handle<v8::Value> async_atomic_div(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_mod(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_atomic_mod(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_and(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_and(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_or(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_or(const v8::Arguments& args);
		static v8::Handle<v8::Value> atomic_xor(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomic_xor(const v8::Arguments& args);
		static v8::Handle<v8::Value> string_prepend(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_string_prepend(const v8::Arguments& args);
		static v8::Handle<v8::Value> string_append(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_string_append(const v8::Arguments& args);

		static v8::Handle<v8::Value> list_lpush(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_list_lpush(const v8::Arguments& args);
		static v8::Handle<v8::Value> list_rpush(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_list_rpush(const v8::Arguments& args);

		static v8::Handle<v8::Value> set_add(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_set_add(const v8::Arguments& args);
		static v8::Handle<v8::Value> set_remove(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_set_remove(const v8::Arguments& args);
		static v8::Handle<v8::Value> set_union(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_set_union(const v8::Arguments& args);
		static v8::Handle<v8::Value> set_intersect(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_set_intersect(const v8::Arguments& args);

		static v8::Handle<v8::Value> map_add(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_add(const v8::Arguments& args);
		static v8::Handle<v8::Value> map_remove(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_remove(const v8::Arguments& args);
		static v8::Handle<v8::Value> map_atomic_add(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_add(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_sub(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_sub(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_mul(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_mul(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_div(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_div(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_mod(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_mod(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_and(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_and(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_or(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_or(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_atomic_xor(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_atomic_xor(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_string_prepend(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_string_prepend(const v8::Arguments& args);
                static v8::Handle<v8::Value> map_string_append(const v8::Arguments& args);
                static v8::Handle<v8::Value> async_map_string_append(const v8::Arguments& args);

		static v8::Handle<v8::Value> async_execute_op(const v8::Arguments& args, enum Node_Op);
		static v8::Handle<v8::Value> execute_op(const v8::Arguments& args, enum Node_Op);
		
		static v8::Handle<v8::Value> async_execute_map_op(const v8::Arguments& args, enum Node_Op);
                static v8::Handle<v8::Value> execute_map_op(const v8::Arguments& args, enum Node_Op);
		
		static void copy_client(HyperClient** o1, HyperClient** o2);
		v8::Local<v8::Array> ops;
                v8::Local<v8::Array> result;
		
 	private:
  		HyperClient();
  		~HyperClient();
		struct hyperclient *client;
		struct hyperclient_attribute **attrs;
		size_t *attrs_size;
		enum hyperclient_returncode *ret;
		int op;
		int64_t val;
  		static v8::Persistent<v8::Function> constructor;
  		static v8::Handle<v8::Value> New(const v8::Arguments& args);

};

Handle<Value> CreateObject(const Arguments& args) {
  HandleScope scope;
  return scope.Close(HyperClient::NewInstance(args));
}

void InitAll(Handle<Object> target) {
  	HyperClient::Init();

  	target->Set(String::NewSymbol("Client"), FunctionTemplate::New(CreateObject)->GetFunction());

}

NODE_MODULE(hyperclient, InitAll)

struct hc_attrs
{
    bool isAttr;
    struct hyperclient_attribute* attrs;
    size_t attrs_size;
    bool retval;
};

std::map <int64_t, int32_t> map_ops;
std::map <int64_t, std::list<struct hc_attrs* > * > map_attrs;

typedef std::pair<int64_t, int32_t> pair_ops;
typedef std::pair<int64_t, std::list<struct hc_attrs* > * > pair_attrs;
HyperClient::HyperClient() {};
HyperClient::~HyperClient()
{
};

Persistent<Function> HyperClient::constructor;



void HyperClient::Init() {
    // Prepare constructor template
    Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
    tpl->SetClassName(String::NewSymbol("HyperClient"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    // Prototype
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_get"), FunctionTemplate::New(async_get)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("get"), FunctionTemplate::New(get)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_put"), FunctionTemplate::New(async_put)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("put"), FunctionTemplate::New(put)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("wait"), FunctionTemplate::New(wait)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("destroy"), FunctionTemplate::New(destroy)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_condput"), FunctionTemplate::New(async_condput)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("condput"), FunctionTemplate::New(condput)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_search"), FunctionTemplate::New(async_search)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("search"), FunctionTemplate::New(search)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_delete"), FunctionTemplate::New(async_del)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("delete"), FunctionTemplate::New(del)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_add"), FunctionTemplate::New(async_atomic_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_add"), FunctionTemplate::New(atomic_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atmoic_sub"), FunctionTemplate::New(async_atomic_sub)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_sub"), FunctionTemplate::New(atomic_sub)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_mul"), FunctionTemplate::New(async_atomic_mul)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_mul"), FunctionTemplate::New(atomic_mul)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_div"), FunctionTemplate::New(async_atomic_div)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_div"), FunctionTemplate::New(atomic_div)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_mod"), FunctionTemplate::New(async_atomic_mod)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_mod"), FunctionTemplate::New(atomic_mod)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_and"), FunctionTemplate::New(async_atomic_and)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_and"), FunctionTemplate::New(atomic_and)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_or"), FunctionTemplate::New(async_atomic_or)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_or"), FunctionTemplate::New(atomic_or)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomic_xor"), FunctionTemplate::New(async_atomic_xor)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("atomic_xor"), FunctionTemplate::New(atomic_xor)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_string_prepend"), FunctionTemplate::New(async_string_prepend)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("string_prepend"), FunctionTemplate::New(string_prepend)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_string_append"), FunctionTemplate::New(async_string_append)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("string_append"), FunctionTemplate::New(string_append)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_list_lpush"), FunctionTemplate::New(async_list_lpush)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("list_lpush"), FunctionTemplate::New(list_lpush)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_list_rpush"), FunctionTemplate::New(async_list_rpush)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("list_rpush"), FunctionTemplate::New(list_rpush)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_set_add"), FunctionTemplate::New(async_set_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("set_add"), FunctionTemplate::New(set_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_set_remove"), FunctionTemplate::New(async_set_remove)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("set_remove"), FunctionTemplate::New(set_remove)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_set_union"), FunctionTemplate::New(async_set_union)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("set_union"), FunctionTemplate::New(set_union)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_set_intersect"), FunctionTemplate::New(async_set_intersect)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("set_intersect"), FunctionTemplate::New(set_intersect)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_add"), FunctionTemplate::New(async_map_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_add"), FunctionTemplate::New(map_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_remove"), FunctionTemplate::New(async_map_remove)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_remove"), FunctionTemplate::New(map_remove)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_add"), FunctionTemplate::New(async_map_atomic_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_add"), FunctionTemplate::New(map_atomic_add)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_sub"), FunctionTemplate::New(async_map_atomic_sub)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_sub"), FunctionTemplate::New(map_atomic_sub)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_mul"), FunctionTemplate::New(async_map_atomic_mul)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_mul"), FunctionTemplate::New(map_atomic_mul)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_div"), FunctionTemplate::New(async_map_atomic_div)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_div"), FunctionTemplate::New(map_atomic_div)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_mod"), FunctionTemplate::New(async_map_atomic_mod)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_mod"), FunctionTemplate::New(map_atomic_mod)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_and"), FunctionTemplate::New(async_map_atomic_and)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_and"), FunctionTemplate::New(map_atomic_and)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_or"), FunctionTemplate::New(async_map_atomic_or)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_or"), FunctionTemplate::New(map_atomic_or)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_atomic_xor"), FunctionTemplate::New(async_map_atomic_xor)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_atomic_xor"), FunctionTemplate::New(map_atomic_xor)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_string_prepend"), FunctionTemplate::New(async_map_string_prepend)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_string_prepend"), FunctionTemplate::New(map_string_prepend)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("async_map_string_append"), FunctionTemplate::New(async_map_string_append)->GetFunction());
    tpl->PrototypeTemplate()->Set(String::NewSymbol("map_string_append"), FunctionTemplate::New(map_string_append)->GetFunction());

    constructor = Persistent<Function>::New(tpl->GetFunction());
}

bool isSet(Local<Value> val)
{
    Local<Object> obj = Local<Object>::Cast(val);
    Local<Array> arr = Local<Array>::Cast(obj->Get(String::New("_items")));
    if(arr->IsArray())
    {
        return true;
    }
    else
    {
        return false;
    }
}


std::string tostr(Local<Value> str)
{
        return cvv8::CastFromJS<std::string>(str->ToString());
}

char *getstr(Local<Value> str)
{
        std::string foo;
        char *newstr;
        foo = cvv8::CastFromJS<std::string>(str->ToString());
        newstr = (char*) malloc(foo.length());
        foo.copy(newstr, foo.length(), 0);
        return newstr;
}

int32_t toint32(Local<Value> val)
{
        return cvv8::CastFromJS<int32_t>(val->ToInteger());
}

Handle<Value> newint(int32_t val)
{
        return cvv8::CastToJS(val);
}

int64_t toint64(Local<Value> val)
{
        return cvv8::CastFromJS<int64_t>(val->ToInteger());
}

Handle<Value> newint(int64_t val)
{
        return cvv8::CastToJS(val);
}

Local<String> newstr(const char* str)
{
        return String::New(str);
}

void exception(const char* str)
{
        ThrowException(Exception::Error(newstr(str)));
}

void raise_exception(enum hyperclient_returncode ret)
{
        switch(ret)
        {

                case HYPERCLIENT_NOTFOUND:
                        exception("Not Found");
                        break;
                case HYPERCLIENT_SEARCHDONE:
                        exception("Search Done");
                        break;
                case HYPERCLIENT_CMPFAIL:
                        exception("Conditional Operation Did Not Match Object");
                        break;
                case HYPERCLIENT_UNKNOWNSPACE:
                        exception("Unknown Space");
                        break;
                case HYPERCLIENT_COORDFAIL:
                        exception("Coordinator Failure");
                        break;
                case HYPERCLIENT_SERVERERROR:
                        exception("Server Error");
                        break;
                case HYPERCLIENT_RECONFIGURE:
                        exception("Reconfiguration");
                        break;
                case HYPERCLIENT_TIMEOUT:
                        exception("Timeout");
                        break;
                case HYPERCLIENT_UNKNOWNATTR:
                        exception("Unknown attribute");
                        break;
                case HYPERCLIENT_DUPEATTR:
                        exception("Duplicate attribute ");
                        break;
                case HYPERCLIENT_NONEPENDING:
                        exception("None pending");
                        break;
                case HYPERCLIENT_DONTUSEKEY:
                        exception("Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
                        break;
                case HYPERCLIENT_WRONGTYPE:
                        exception("Attribute has the wrong type");
                        break;
                case HYPERCLIENT_EXCEPTION:
                        exception("Internal Error (file a bug)");
                        break;
        }
}



void ins_ops(int64_t key, int32_t val)
{
    map_ops.erase(key);
    map_ops.insert(pair_ops(key, val));
    return;
}

int32_t get_ops(int64_t key)
{
    return map_ops[key];
}

void ins_attrs(int64_t key, std::list<struct hc_attrs* > * val)
{
    map_attrs.erase(key);
    map_attrs.insert(pair_attrs(key, val));
    return;
}

std::list<struct hc_attrs*>* get_attrs(int64_t key)
{
    return map_attrs[key];
}


char* copy_n(const char* str, int n)
{
        int i;
        char * str1;
        str1 = (char *) malloc(n+1);
        for(i=0;i<n;i++)
                str1[i] = str[i];
        str1[i] = '\0';
        return str1;
}

void destroy_attrs(struct hyperclient_attribute** a, size_t size)
{
        int i;
        for(i=0;i<size;i++)
        {
        if((*a)[i].attr != NULL)
            free((void *)(*a)[i].attr);
        if((*a)[i].value != NULL)
                    free((void *)(*a)[i].value);
        }
        return;
}


Handle<Value> HyperClient::New(const Arguments& args) {
    if(args.Length() != 2)
    {
        exception("Wrong Arguments");
        return v8::Undefined();
    }
    HyperClient* obj = new HyperClient();
    obj->client = hyperclient_create(tostr(args[0]).c_str(), toint32(args[1]));
    obj->attrs = (struct hyperclient_attribute**) malloc(sizeof(struct hyperclient_attribute*));
    obj->attrs_size = (size_t*) malloc(sizeof(size_t));
    obj->ret = (enum hyperclient_returncode*) malloc(sizeof(enum hyperclient_returncode));
    obj->ops = Array::New();
    obj->result = Array::New();
    obj->Wrap(args.This());

    return args.This();
}

Handle<Value> HyperClient::NewInstance(const Arguments& args) {
    HandleScope scope;

    const unsigned argc = 2;
    Handle<Value> argv[argc] = { args[0], args[1] };
    Local<Object> instance = constructor->NewInstance(argc, argv);

    return scope.Close(instance);
}


void dict_to_map_attrs(Local<Object> obj, struct hyperclient_map_attribute** a, size_t* a_sz)
{
    Local<Value> key, map_key, map_val;
    Local<Object> val;
    Local<Array> arr = obj->GetPropertyNames();
    Local<Array> map_arr;
    int i, j, k, count;
    int ktype, vtype, dtype;
    int64_t num;
    count = 0;
    for(i = 0; i<arr->Length(); i++)
    {
        key = arr->Get(i);
        val = Local<Object>::Cast(obj->Get(key));
        map_arr = Local<Array>::Cast(val->GetPropertyNames());
        count += map_arr->Length();
    }
    a[0] = (struct hyperclient_map_attribute*) malloc(count*(sizeof(struct hyperclient_map_attribute)));
    a_sz[0] = count;
    k = 0;
    for(i = 0; i<arr->Length(); i++)
    {
        key = arr->Get(i);
        if(!key->IsString())
        {
            exception("Invalid Key Type");
        }
                val = Local<Object>::Cast(obj->Get(key));
        map_arr = Local<Array>::Cast(val->GetPropertyNames());
        ktype = 0;
        vtype = 0;
        for(j = 0; j < map_arr->Length(); j++)
        {
            map_key = map_arr->Get(j);
            map_val = val->Get(map_key);
            a[0][k].attr = getstr(key);
            if(map_key->IsString() && (ktype == 0 || ktype == 1))
            {
                a[0][k].map_key = getstr(map_key);
                a[0][k].map_key_sz = strlen(a[0][k].map_key);
                ktype = 1;
            }
            else if(map_key->IsNumber() && (ktype == 0 || ktype == 2))
            {
                num = cvv8::CastFromJS<int64_t>(map_key->ToInteger());
                            num = htole64(num);
                            a[0][k].map_key = (char *)malloc(8*sizeof(char));
                            memmove((void*)a[0][k].map_key, &num, (size_t)8);
                            a[0][k].map_key_sz = 8;
                ktype = 2;
            }
            else
            {
                exception("Mixed Type");
            }
            if(map_val->IsString() && (vtype == 0 || vtype == 3))
                    {
                a[0][k].value = getstr(map_val);
                                a[0][k].value_sz = strlen(a[0][k].value);
                            vtype = 3;
                    }
                    else if(map_val->IsNumber() && (vtype == 0 || vtype == 5))
                    {
                num = cvv8::CastFromJS<int64_t>(map_val->ToInteger());
                                num = htole64(num);
                                a[0][k].value = (char *)malloc(8*sizeof(char));
                                memmove((void*)a[0][k].value, &num, (size_t)8);
                                a[0][k].value_sz = 8;
                            vtype = 5;
                    }
                    else
                    {
                            exception("Mixed Type");
                    }
            dtype = ktype + vtype;
            switch(dtype)
            {
                case 4:
                    a[0][k].datatype = HYPERDATATYPE_MAP_STRING_STRING;
                break;
                                case 5:
                                        a[0][k].datatype = HYPERDATATYPE_MAP_INT64_STRING;
                                break;
                                case 6:
                                        a[0][k].datatype = HYPERDATATYPE_MAP_STRING_INT64;
                                break;
                                case 7:
                                        a[0][k].datatype = HYPERDATATYPE_MAP_INT64_INT64;
                                break;
            }
            k++;
        }
    }
}

void dict_to_attrs(Local<Object> obj, int isinc, struct hyperclient_attribute** a)
{
    int i, j, pos, len, templen;
    int64_t num;
    char *ptr;
    Local<Value> key, val;
    Local<Array> arr = obj->GetPropertyNames();
//  printf("Length : %d\n", arr->Length());
    for(i = 0; i<arr->Length(); i++)
    {
        key = arr->Get(i);
                val = obj->Get(key);
            if(key->IsString())
                {
            (*a)[i].attr = getstr(key);
//          printf("%s : ",(*a)[i].attr);
                }
                else
                {
            raise_exception(HYPERCLIENT_WRONGTYPE);
                }

                if(val->IsString())
                {
            (*a)[i].value = getstr(val);
//          printf("%s\n",(*a)[i].value);
            (*a)[i].value_sz = strlen((*a)[i].value);
            (*a)[i].datatype = HYPERDATATYPE_STRING;
                }
                else if(val->IsNumber())
                {
                    num = cvv8::CastFromJS<int64_t>(val->ToInteger());
            if(!(isinc))
                num = -num;
//          printf("%ld\n",num);
            num = htole64(num);
            (*a)[i].value = (char *)malloc(8*sizeof(char));
            memmove((void*)(*a)[i].value, &num, (size_t)8);
            (*a)[i].value_sz = 8;
            (*a)[i].datatype = HYPERDATATYPE_INT64;
                }
        else if(isSet(val))
        {
//          printf("Set\n");
            Local<Array> set_arr = Local<Array>::Cast((Local<Object>::Cast(val))->Get(newstr("_items")));
            if(set_arr->Length() != 0)
            {
                Local<Value> tempval = set_arr->Get(0);
                if(tempval->IsNumber())
                {
                    pos = 0;
                    (*a)[i].value = (char*) malloc(8*set_arr->Length());
                    (*a)[i].value_sz = 8*set_arr->Length();
                                        (*a)[i].datatype = HYPERDATATYPE_SET_INT64;
                    for( j = 0; j < set_arr->Length(); j++)
                    {
                        tempval = set_arr->Get(j);
                        if(!(tempval->IsNumber()))
                        {
                            free((void *) (*a)[i].value);
                            exception("Invalid datatype in the Set");
                        }
                        num = cvv8::CastFromJS<int64_t>(tempval->ToInteger());
                        num = htole64(num);
                        memmove((void *)((*a)[i].value+pos), &num, (size_t)8);
                        pos += 8;
                    }
                }
                else if(tempval->IsString())
                {
                    pos = 0;
                    len = 0;
                    templen;
                    std::string tempstr;
                    for(j = 0; j < set_arr->Length(); j++)
                    {
                        tempval = set_arr->Get(j);
                        if(!(tempval->IsString()))
                        {
                            exception("Invalid datatype in the Set");
                        }
                        tempstr = tostr(tempval);
                        len += tempstr.length();
                    }
                    (*a)[i].value = (char *) malloc((4*set_arr->Length()) + len);
                    (*a)[i].value_sz = (4*set_arr->Length()) + len;
                    (*a)[i].datatype = HYPERDATATYPE_SET_STRING;
                    for(j = 0; j < set_arr->Length(); j++)
                                    {
                        tempval = set_arr->Get(j);
                        tempstr = tostr(tempval);
                                    len = tempstr.length();
                        templen = htole32(len);
                        memmove((void *)((*a)[i].value + pos), &templen, (size_t)4);
                        tempstr.copy((char *)((*a)[i].value + pos + 4), len, 0);
                        pos += len + 4;
                                    }
                }
                else
                {
                    exception("Invalid Datatype");
                }
            }
            else
            {
                (*a)[i].value = NULL;
                (*a)[i].value_sz = 0;
                (*a)[i].datatype = HYPERDATATYPE_SET_STRING;
            }
        }
        else if(val->IsArray())
        {
//          printf("Array\n");
                        Local<Array> temparr = Local<Array>::Cast(val);
            if(temparr->Length() != 0)
            {
                            Local<Value> tempval = temparr->Get(0);
                            if(tempval->IsNumber())
                            {
//                              printf("Number\n");
                                    pos = 0;
                                    (*a)[i].value = (char*) malloc(8*temparr->Length());
                    (*a)[i].value_sz = 8*temparr->Length();
                    (*a)[i].datatype = HYPERDATATYPE_LIST_INT64;
                                    for( j = 0; j < temparr->Length(); j++)
                                    {
                                            tempval = temparr->Get(j);
                                            if(!(tempval->IsNumber()))
                                            {
                                                    free((void *) (*a)[i].value);
                                                    exception("Invalid datatype in the Set");
                                            }
                                            num = cvv8::CastFromJS<int64_t>(tempval->ToInteger());
                                            num = htole64(num);
                                            memmove((void *)((*a)[i].value+pos), &num, (size_t)8);
                                            pos += 8;
                                    }
                            }
                            else if(tempval->IsString())
                            {
//                  printf("String\n");
                                    pos = 0;
                                    len = 0;
                    templen;
                                    std::string tempstr;
                                    for(j = 0; j < temparr->Length(); j++)
                                    {
                                            tempval = temparr->Get(j);
                                            if(!(tempval->IsString()))
                                            {
                                                    exception("Invalid datatype in the Set");
                                            }
                                            tempstr = tostr(tempval);
                            len += tempstr.length();
                                    }
                    (*a)[i].value_sz = (4*temparr->Length()) + len;
                                    (*a)[i].value = (char *) malloc((*a)[i].value_sz);
                    (*a)[i].datatype = HYPERDATATYPE_LIST_STRING;
                                    for(j = 0; j < temparr->Length(); j++)
                                    {
                                            tempval = temparr->Get(j);
                                            tempstr = tostr(tempval);
                                            len = tempstr.length();
                        templen = htole32(len);
                                            memmove((void *)((*a)[i].value + pos), &len, (size_t)4);
                                            tempstr.copy((char *)((*a)[i].value + pos + 4), len, 0);
                                            pos += len + 4;
                                    }
                            }
                            else
                            {
                                    exception("Invalid Datatype");
                            }
            }
            else
            {
                (*a)[i].value = NULL;
                (*a)[i].value_sz = 0;
                (*a)[i].datatype = HYPERDATATYPE_LIST_STRING;
            }
//          printf("Array done\n");
        }
        else if(val->IsObject())
        {
//          printf("Dict\n");
            Local<Object> map_obj = Local<Object>::Cast(val);
            Local<Array> map_arr = map_obj->GetPropertyNames();
            Local<Value> map_key, map_val;
            if(map_arr->Length() != 0)
            {
                map_key = map_arr->Get(0);
                map_val = map_obj->Get(map_key);
                if(map_key->IsString() && map_val->IsString())
                {
                    (*a)[i].datatype = HYPERDATATYPE_MAP_STRING_STRING;
                }
                else if(map_key->IsString() && map_val->IsNumber())
                {
                    (*a)[i].datatype = HYPERDATATYPE_MAP_STRING_INT64;
                }
                else if(map_key->IsNumber() && map_val->IsString())
                {
                    (*a)[i].datatype = HYPERDATATYPE_MAP_INT64_STRING;
                }
                else if(map_key->IsNumber() && map_val->IsNumber())
                {
                    (*a)[i].datatype = HYPERDATATYPE_MAP_INT64_INT64;
                }
                else
                {
                    exception("Invalid types");
                }
                std::string tempstr1, tempstr2;
                                len = 0;
                                pos = 0;
                switch((*a)[i].datatype)
                {
                    case HYPERDATATYPE_MAP_STRING_STRING:
                        for(j = 0; j < map_arr->Length(); j++)
                        {
                            map_key = map_arr->Get(j);
                            map_val = map_obj->Get(map_key);
                            if(!(map_key->IsString() && map_val->IsString()))
                            {
                                exception("Mixed Type");
                            }
                            tempstr1 = tostr(map_key);
                            tempstr2 = tostr(map_val);
                            len += tempstr1.length();
                            len += tempstr2.length();
                        }
                        (*a)[i].value = (char *) malloc(8*map_arr->Length() + len);
                        (*a)[i].value_sz = 8*map_arr->Length() + len;
                        for(j = 0; j < map_arr->Length(); j++)
                                            {
                                                    map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                                                    tempstr1 = tostr(map_key);
                                                    tempstr2 = tostr(map_val);
                            len = tempstr1.length();
                            templen = htole32(len);
                            memmove((void *)((*a)[i].value + pos), &templen, 4);
                            tempstr1.copy((char *)((*a)[i].value + pos + 4), len, 0);
                            pos += len + 4;
                                                    len = tempstr2.length();
                            templen = htole32(len);
                            memmove((void *)((*a)[i].value + pos), &templen, 4);
                            tempstr2.copy((char *)((*a)[i].value + pos + 4), len, 0);
                            pos += len + 4;
                                            }
                    break;
                    case HYPERDATATYPE_MAP_STRING_INT64:
                        for(j = 0; j < map_arr->Length(); j++)
                                            {
                                                    map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                                                    if(!(map_key->IsString() && map_val->IsNumber()))
                                                    {
                                                            exception("Mixed Type");
                                                    }
                                                    tempstr1 = tostr(map_key);
                                                    len += tempstr1.length();
                                            }
                                            (*a)[i].value = (char *) malloc(12*map_arr->Length() + len);
                                            (*a)[i].value_sz = 12*map_arr->Length() + len;
                                            for(j = 0; j < map_arr->Length(); j++)
                                            {
                                                    map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                                                    tempstr1 = tostr(map_key);
                                                    len = tempstr1.length();
                                                    templen = htole32(len);
                                                    memmove((void *)((*a)[i].value + pos), &templen, 4);
                                                    tempstr1.copy((char *)((*a)[i].value + pos + 4), len, 0);
                            pos += len + 4;
                            num = cvv8::CastFromJS<int64_t>(map_val->ToInteger());
                                                num = htole64(num);
                                                memmove((void *)((*a)[i].value + pos), &num, (size_t)8);
                            pos += 8;
                                            }
                    break;
                    case HYPERDATATYPE_MAP_INT64_STRING:
                        for(j = 0; j < map_arr->Length(); j++)
                                            {
                                                    map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                                                    if(!(map_key->IsNumber() && map_val->IsString()))
                                                    {
                                                            exception("Mixed Type");
                                                    }
                                                    tempstr1 = tostr(map_val);
                                                    len += tempstr1.length();
                                            }
                                            (*a)[i].value = (char *) malloc(12*map_arr->Length() + len);
                                            (*a)[i].value_sz = 12*map_arr->Length() + len;
                                            for(j = 0; j < map_arr->Length(); j++)
                                            {
                                                    map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                            num = cvv8::CastFromJS<int64_t>(map_key->ToInteger());
                                                    num = htole64(num);
                                                    memmove((void *)((*a)[i].value + pos), &num, (size_t)8);
                                                    pos += 8;
                                                    tempstr1 = tostr(map_val);
                                                    len = tempstr1.length();
                                                    templen = htole32(len);
                                                    memmove((void *)((*a)[i].value + pos), &templen, 4);
                                                    tempstr1.copy((char *)((*a)[i].value + pos + 4), len, 0);
                                                    pos += len + 4;
                                            }
                    break;
                    case HYPERDATATYPE_MAP_INT64_INT64:
                        (*a)[i].value = (char *) malloc(16*map_arr->Length());
                        (*a)[i].value_sz = 16*map_arr->Length();
                        for(j = 0; j < map_arr->Length() ; j++)
                        {
                            map_key = map_arr->Get(j);
                                                    map_val = map_obj->Get(map_key);
                            if(!(map_key->IsNumber() && map_val->IsNumber()))
                                                    {
                                                            exception("Mixed Type");
                                                    }
                                                    num = cvv8::CastFromJS<int64_t>(map_key->ToInteger());
                                                    num = htole64(num);
                                                    memmove((void *)((*a)[i].value + pos), &num, (size_t)8);
                                                    pos += 8;
                            num = cvv8::CastFromJS<int64_t>(map_val->ToInteger());
                                                    num = htole64(num);
                                                    memmove((void *)((*a)[i].value + pos), &num, (size_t)8);
                                                    pos += 8;
                        }
                    break;
                }
            }
            else
            {
                (*a)[i].value = NULL;
                (*a)[i].value_sz = 0;
                (*a)[i].datatype = HYPERDATATYPE_MAP_STRING_STRING;
            }
        }
        else
        {
            exception("Unknown Datatype");
            return;
        }
    }
    return;
}


Local<Array> attrs_to_dict(struct hyperclient_attribute* attrs, size_t attrs_size)
{
    int i;
    Local<Array> obj = Array::New();
    Local<Value> key, val, map_key, map_val;
    const char* str;
    int64_t num;
    int pos, rem, arr_pos;
    for(i=0;i<attrs_size;i++)
    {
        key = newstr((char *)attrs[i].attr);
        if(attrs[i].attr[0] == '\n')
        {
            printf("Yes\n");
        }
        if(attrs[i].datatype == HYPERDATATYPE_STRING)
        {
            if(attrs[i].value_sz != 0)
            {
                str = (const char *)copy_n(attrs[i].value, attrs[i].value_sz);
                val = newstr(str);
                free((void *)str);
            }
            else
            {
                str = (const char *)"";
                val = newstr(str);
            }
//          printf("%s\n", str);
        }
        else if(attrs[i].datatype == HYPERDATATYPE_INT64)
        {
            if(attrs[i].value_sz != 0)
            {
                memmove(&num, attrs[i].value, attrs[i].value_sz);
                                num = le64toh(num);
//              printf("%ld\n", num);
                val = Integer::New(num);
            }
            else
            {
                val = Integer::New(0);
            }
        }
        else if(attrs[i].datatype == HYPERDATATYPE_LIST_STRING)
        {
            Local<Array> arr = Array::New();
            pos = 0;
            arr_pos = 0;
            rem = attrs[i].value_sz;
            while(rem >= 4)
            {
                memmove(&num, attrs[i].value + pos, 4);
                num = le32toh(num);
                if((rem - 4) < num)
                {
                    exception("list(string) is improperly structured");
                }
                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                arr->Set(arr_pos, newstr(str));
                free((void*) str);
                pos += 4 + num;
                rem -= 4 + num;
                arr_pos++;
            }
            if(rem > 0)
            {
                exception("list(string) contains excess data");
            }
            val = arr;
        }
        else if(attrs[i].datatype == HYPERDATATYPE_SET_STRING)
        {
            Local<Object> set_obj = Object::New();
            Local<Array> arr = Array::New();
                        pos = 0;
                        arr_pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 4)
                        {
                                memmove(&num, attrs[i].value + pos, 4);
                                num = le32toh(num);
                                if((rem - 4) < num)
                                {
                                        exception("list(string) is improperly structured");
                                }
                                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                                arr->Set(arr_pos, newstr(str));
                                free((void*) str);
                                pos += 4 + num;
                                rem -= 4 + num;
                                arr_pos++;
                        }
                        if(rem > 0)
                        {
                                exception("list(string) contains excess data");
                        }
            set_obj->Set(newstr("_items"), arr);
                        val = set_obj;
        }
        else if(attrs[i].datatype == HYPERDATATYPE_LIST_INT64)
        {
            Local<Array> arr = Array::New();
                        pos = 0;
                        arr_pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 8)
                        {
                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                arr->Set(arr_pos, Integer::New(num));
                pos += 8;
                rem -= 8;
                arr_pos++;
            }
            if(rem > 0)
                        {
                                exception("list(string) contains excess data");
                        }
                        val = arr;
        }
        else if(attrs[i].datatype == HYPERDATATYPE_SET_INT64)
        {
            Local<Array> arr = Array::New();
            Local<Object> set_obj = Object::New();
                        pos = 0;
                        arr_pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 8)
                        {
                                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                                arr->Set(arr_pos, Integer::New(num));
                                pos += 8;
                                rem -= 8;
                                arr_pos++;
                        }
                        if(rem > 0)
                        {
                                exception("list(string) contains excess data");
                        }
            set_obj->Set(newstr("_items"), arr);
                        val = set_obj;
        }
        else if(attrs[i].datatype == HYPERDATATYPE_MAP_STRING_STRING)
        {
            Local<Object> map_obj = Object::New();
            pos = 0;
            rem = attrs[i].value_sz;
            while(rem >= 4)
            {
                memmove(&num, attrs[i].value + pos, 4);
                                num = le32toh(num);
                                if((rem - 4) < num)
                                {
                                        exception("map(string,string) is improperly structured");
                                }
                                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                                map_key = newstr(str);
                                free((void*) str);
                                pos += 4 + num;
                                rem -= 4 + num;
                memmove(&num, attrs[i].value + pos, 4);
                                num = le32toh(num);
                                if((rem - 4) < num)
                                {
                                        exception("map(string, string) is improperly structured");
                                }
                                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                                map_val = newstr(str);
                                free((void*) str);
                                pos += 4 + num;
                                rem -= 4 + num;
                map_obj->Set(map_key, map_val);
            }
            val = map_obj;
        }
        else if(attrs[i].datatype == HYPERDATATYPE_MAP_STRING_INT64)
                {
            Local<Object> map_obj = Object::New();
                        pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 4)
                        {
                                memmove(&num, attrs[i].value + pos, 4);
                                num = le32toh(num);
                                if((rem - 4) < num)
                                {
                                        exception("map(string, int64) is improperly structured");
                                }
                                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                                map_key = newstr(str);
                                free((void*) str);
                                pos += 4 + num;
                                rem -= 4 + num;
                if(rem < 8)
                {
                    exception("map(string, int64) is improperly structured");
                }
                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                                map_val = Integer::New(num);
                                pos += 8;
                                rem -= 8;
                map_obj->Set(map_key, map_val);
            }
            val = map_obj;
                }
        else if(attrs[i].datatype == HYPERDATATYPE_MAP_INT64_STRING)
                {
            Local<Object> map_obj = Object::New();
                        pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 8)
                        {
                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                                map_key = Integer::New(num);
                                pos += 8;
                                rem -= 8;
                memmove(&num, attrs[i].value + pos, 4);
                                num = le32toh(num);
                                if((rem - 4) < num)
                                {
                                        exception("map(string, int64) is improperly structured");
                                }
                                str = (const char *)copy_n(attrs[i].value + pos + 4, num);
                                map_val = newstr(str);
                                free((void*) str);
                                pos += 4 + num;
                                rem -= 4 + num;
                map_obj->Set(map_key, map_val);
            }
            val = map_obj;
                }
        else if(attrs[i].datatype == HYPERDATATYPE_MAP_INT64_INT64)
                {
            Local<Object> map_obj = Object::New();
                        pos = 0;
                        rem = attrs[i].value_sz;
                        while(rem >= 8)
                        {
                                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                                map_key = Integer::New(num);
                                pos += 8;
                                rem -= 8;
                if(rem < 8)
                                {
                                        exception("map(string, int64) is improperly structured");
                                }
                                memmove(&num, attrs[i].value + pos, 8);
                                num = le64toh(num);
                                map_val = Integer::New(num);
                                pos += 8;
                                rem -= 8;
                                map_obj->Set(map_key, map_val);
            }
            val = map_obj;
        }
        else
        {
            Local<Array> nullarr;
            //Wrong type
            raise_exception(HYPERCLIENT_WRONGTYPE);
            return nullarr;
        }
        obj->Set(key, val);
    }
    return obj;
}

Local<Value> arr_shift(Local<Array> arr)
{
    int i=0;
    Local<Value> val;
    while(val->IsUndefined())
    {
        val = arr->Get(i);
        i++;
    }
    arr->Delete(i-1);
    return val;
}

void arr_unshift(Local<Array> arr, Local<Value> val)
{
    int i = arr->Length();
    arr->Set(i, val);
}



Handle<Value> HyperClient::wait(const Arguments& args)
{
    HyperClient* obj = ObjectWrap::Unwrap<HyperClient>(args.This());
    return obj->wait();
}

Handle<Value> HyperClient::wait()
{
    HyperClient* obj = this;
    int64_t val;
    int32_t op=-1;
    std::list<struct hc_attrs* > * hc_list;
    struct hc_attrs* attr = NULL;
    Handle<Value> ret;
    hc_list = get_attrs(obj->val);
    op = get_ops(obj->val);
    if(hc_list != NULL)
    {
//      printf("Val = %ld, OP = %d, list size = %zd\n", obj->val, op, hc_list->size());
        if(hc_list->size() != 0)
        {
            attr = hc_list->front();
            hc_list->pop_front();
            ins_attrs(obj->val, hc_list);
            if(op == 0)
                    {
                            delete hc_list;
                            map_attrs.erase(obj->val);
                    }
            if(attr->isAttr == true)
            {
                ret = attrs_to_dict(attr->attrs, attr->attrs_size);
                        hyperclient_destroy_attrs(attr->attrs, attr->attrs_size);
            }
            else
            {
                ret = Boolean::New(attr->retval);
            }
            free(attr);
            return ret;
        }
        else if(op == 0)
        {
            delete hc_list;
            map_attrs.erase(obj->val);
            return v8::Undefined();
        }
    }
    do
    {
        val = hyperclient_loop(obj->client, -1, obj->ret);
        if(*(obj->ret) != HYPERCLIENT_SUCCESS && *(obj->ret) != HYPERCLIENT_NOTFOUND && *(obj->ret) != HYPERCLIENT_CMPFAIL && *(obj->ret) != HYPERCLIENT_SEARCHDONE)
        {
            raise_exception(*(obj->ret));
            return v8::Undefined();
        }
        hc_list = get_attrs(val);
        if(hc_list == NULL)
        {
            hc_list = new std::list<struct hc_attrs*>();
        }
        op = get_ops(val);
//      printf("list size = %zd, Val = %ld, OP = %d\n",hc_list->size(), val, op);
        if(op == 1)
        {
            attr = (struct hc_attrs*) malloc(sizeof(struct hc_attrs));
            if(*(obj->ret) == HYPERCLIENT_SUCCESS)
            {
                attr->isAttr = true;
                attr->attrs = *(obj->attrs);
                attr->attrs_size = *(obj->attrs_size);
            }
            else
            {
                attr->isAttr = false;
                attr->retval = false;
            }
            ins_ops(val, 0);
                        hc_list->push_back(attr);
                        ins_attrs(val, hc_list);
        }
        else if(op == 7)
        {
            attr = (struct hc_attrs*) malloc(sizeof(struct hc_attrs));
                        if(*(obj->ret) == HYPERCLIENT_SUCCESS)
                        {
                                attr->isAttr = true;
                                attr->attrs = *(obj->attrs);
                                attr->attrs_size = *(obj->attrs_size);
                        }
            else if(*(obj->ret) == HYPERCLIENT_SEARCHDONE)
            {
                attr->isAttr = false;
                                attr->retval = false;
                ins_ops(val, 0);
            }
                        else
                        {
                                attr->isAttr = false;
                                attr->retval = false;
                ins_ops(val, 0);
                        }
                        hc_list->push_back(attr);
                        ins_attrs(val, hc_list);
        }
        else
        {
            attr = (struct hc_attrs*) malloc(sizeof(struct hc_attrs));
            if(*(obj->ret) == HYPERCLIENT_SUCCESS)
            {
                attr->isAttr = false;
                                attr->retval = true;
            }
            else
            {
                attr->isAttr = false;
                                attr->retval = false;
            }
            ins_ops(val, 0);
                        hc_list->push_back(attr);
                        ins_attrs(val, hc_list);
        }
    }while(val != obj->val);
    attr = hc_list->front();
    hc_list->pop_front();
    ins_attrs(val, hc_list);
    op = get_ops(val);
    if(op == 0)
    {
        delete hc_list;
        map_attrs.erase(obj->val);
    }
    if(attr->isAttr)
        {
        ret = attrs_to_dict(attr->attrs, attr->attrs_size);
//      destroy_attrs(attr->attrs, attr->attrs_size);
        hyperclient_destroy_attrs(attr->attrs, attr->attrs_size);
        }
        else
        {
            ret =  Boolean::New(attr->retval);
        }
    free(attr);
    return ret;
}


Handle<Value> HyperClient::async_condput(const Arguments& args)
{
    HyperClient *obj, *obj1;
    char *space, *key;
    std::string sp, ke;
    size_t key_size, a_sz, b_sz;
    struct hyperclient_attribute *a, *b;
    Local<Array> arr, arr1, arr2, arr3;
    Local<Object> obj2;
    if(args.Length() != 4)
        {
                exception("Wrong Number of Arguments");
                return v8::Undefined();
        }
    sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
        key_size = strlen(key);
    obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);
    arr = Local<Array>::Cast(args[2]);
        arr1 = arr->GetPropertyNames();
        a_sz = (size_t)arr1->Length();
        a = (struct hyperclient_attribute *) malloc(a_sz*sizeof(struct hyperclient_attribute));
        dict_to_attrs(arr, 1, &a);
    arr2 = Local<Array>::Cast(args[3]);
        arr3 = arr2->GetPropertyNames();
        b_sz = (size_t)arr3->Length();
        b = (struct hyperclient_attribute *) malloc(b_sz*sizeof(struct hyperclient_attribute));
        dict_to_attrs(arr2, 1, &b);
    obj->val = hyperclient_condput(obj->client, space, key, key_size, a, a_sz, b, b_sz, obj->ret);
    if(obj->val < 0)
    {
        destroy_attrs(&a, a_sz);
        destroy_attrs(&b, b_sz);
        hyperclient_destroy_attrs(a, a_sz);
        hyperclient_destroy_attrs(b, b_sz);
                raise_exception(*(obj->ret));
                return v8::Undefined();
    }
    obj->op = 3;
    ins_ops(obj->val, obj->op);
    destroy_attrs(&a, a_sz);
    destroy_attrs(&b, b_sz);
        hyperclient_destroy_attrs(a, a_sz);
        hyperclient_destroy_attrs(b, b_sz);
    obj->Wrap(obj2);
    return obj2;
}

Handle<Value> HyperClient::async_del(const Arguments& args)
{
    HandleScope scope;
        std::string sp, ke;
        char *space, *key;
        size_t key_size;
        HyperClient *obj, *obj1;
    Local<Object> obj2;
        if(args.Length() != 2)
        {
                exception("Wrong Number of Arguments");
                return scope.Close(Undefined());
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
//        printf("Space : %s Key : %s\n",space, key);
        key_size = strlen(key);
    obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);
        obj->val = hyperclient_del(obj->client, space, key, key_size, obj->ret);
        if(obj->val < 0)
        {
                raise_exception(*(obj->ret));
                return scope.Close(Undefined());
        }
        obj->op = 4;
    ins_ops(obj->val, obj->op);
    obj->Wrap(obj2);
        return obj2;
}

void HyperClient::copy_client(HyperClient** o1, HyperClient** o2)
{
    (*o1) = new HyperClient();
    (*o1)->client = (*o2)->client;
    (*o1)->attrs = (*o2)->attrs;
    (*o1)->attrs_size = (*o2)->attrs_size;
    (*o1)->ret = (*o2)->ret;
    return;

}

void destroy_range(struct hyperclient_range_query** r, size_t r_sz)
{
    int i;
    for(i = 0; i < r_sz ; i++)
    {
        free((void *)(*r)[i].attr);
    }
    free((void *)(*r));
}
Handle<Value> HyperClient::async_search(const Arguments& args)
{
    HandleScope scope;
    std::string sp, ke;
        char *space;
        size_t sz, a_sz, r_sz;
        HyperClient *obj, *obj1;
    Local<Array> arr, arr1, attr_arr, range_arr, range_arr1;
    Local<Value> k, v, v1, v2;
    Local<Array> v_arr;
    struct hyperclient_attribute* a;
    struct hyperclient_range_query* r;
    int i;
    int64_t num;
    Local<Object> obj2;
        if(args.Length() != 2)
        {
                exception("Wrong Number of Arguments");
                return scope.Close(Undefined());
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
//        printf("Space : %s\n",space);
    obj2 = args.This()->Clone();
    obj = ObjectWrap::Unwrap<HyperClient>(obj2);
    obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
    copy_client(&obj, &obj1);
    arr = Local<Array>::Cast(args[1]);
        arr1 = arr->GetPropertyNames();
        sz = (size_t)arr1->Length();
    a_sz = 0;
    r_sz = 0;
    attr_arr = Array::New();
    range_arr = Array::New();
    for(i = 0; i < sz ; i++)
    {
        k = arr1->Get(i);
                v = arr->Get(k);
                if(v->IsArray())
                {
            range_arr->Set(k, v);
            r_sz++;
                }
                else
                {
            attr_arr->Set(k, v);
            a_sz++;
                }
    }
    a = (struct hyperclient_attribute *) malloc(a_sz*sizeof(struct hyperclient_attribute));
    r = (struct hyperclient_range_query *) malloc(r_sz*sizeof(struct hyperclient_range_query));
    range_arr1 = range_arr->GetPropertyNames();
    dict_to_attrs(attr_arr, 1, &a);
    for(i = 0; i < r_sz; i++)
    {
        k = range_arr1->Get(i);
                v_arr = Local<Array>::Cast(range_arr->Get(k));

                if(k->IsString())
                {
                        r[i].attr = getstr(k);
                }
                else
                {
                        raise_exception(HYPERCLIENT_WRONGTYPE);
                }

        v1 = v_arr->Get(0);
                v2 = v_arr->Get(1);

        if(!v1->IsNumber() || !v2->IsNumber())
        {
            raise_exception(HYPERCLIENT_WRONGTYPE);
            return v8::Undefined();
        }
                else
                {
                        num = cvv8::CastFromJS<int64_t>(v1->ToInteger());
                        num = htole64(num);
                        memmove((void*)&(r[i].lower), &num, (size_t)8);

            num = cvv8::CastFromJS<int64_t>(v2->ToInteger());
                        num = htole64(num);
                        memmove((void*)&(r[i].upper), &num, (size_t)8);
                }
        }
    obj->val = hyperclient_search(obj->client, space, a, a_sz, r, r_sz, obj->ret, obj->attrs, obj->attrs_size);
    if(obj->val < 0)
    {
        destroy_attrs(&a, a_sz);
        hyperclient_destroy_attrs(a, a_sz);
        free(r);
        raise_exception(*(obj->ret));
        return v8::Undefined();
    }
    obj->op = 7;
    ins_ops(obj->val, obj->op);
    destroy_attrs(&a, a_sz);
    hyperclient_destroy_attrs(a, a_sz);
    destroy_range(&r, r_sz);
    obj->Wrap(obj2);
        return obj2;
}


Handle<Value> HyperClient::async_get(const Arguments& args)
{
    HandleScope scope;
    std::string sp, ke;
    char *space, *key;
    size_t key_size;
    HyperClient *obj, *obj1;
    Local<Object> obj2;
    if(args.Length() != 2)
    {
        exception("Wrong Number of Arguments");
        return scope.Close(Undefined());
    }
    sp = tostr(args[0]);
    space = (char *) sp.c_str();
    ke = tostr(args[1]);
    key = (char *) ke.c_str();
//  printf("Space : %s Key : %s\n",space, key);
    key_size = strlen(key);
    obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);
    obj->val = hyperclient_get(obj->client, space, key, key_size, obj->ret, obj->attrs, obj->attrs_size);
    if(obj->val < 0)
    {
        raise_exception(*(obj->ret));
        return v8::Undefined();
    }
    obj->op = 1;
    ins_ops(obj->val, obj->op);
    obj->Wrap(obj2);
    return obj2;
}


Handle<Value> HyperClient::async_put(const Arguments& args)
{
    return HyperClient::async_execute_op(args, PUT);
}


Handle<Value> HyperClient::condput(const Arguments& args)
{
    Handle<Value> client_obj = async_condput(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}
/*
Handle<Value> HyperClient::atomicinc(const Arguments& args)
{
    Handle<Value> client_obj = async_atomicinc(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}

Handle<Value> HyperClient::atomicdec(const Arguments& args)
{
    Handle<Value> client_obj = async_atomicdec(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}
*/
Handle<Value> HyperClient::del(const Arguments& args)
{
    Handle<Value> client_obj = async_del(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}

Handle<Value> HyperClient::search(const Arguments& args)
{
    Handle<Value> client_obj = async_search(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
    Local<Array> arr = Array::New();
    Handle<Value> val;
    int i = 0;
    while((val=obj->wait())!=False())
    {
        arr->Set(i, val);
        i++;
    }
        return arr;
}

Handle<Value> HyperClient::destroy(const Arguments& args)
{
    HyperClient* obj = ObjectWrap::Unwrap<HyperClient>(args.This());
    hyperclient_destroy(obj->client);
    free(obj->attrs);
    free(obj->attrs_size);
    free(obj->ret);
        free(obj);
    return v8::Undefined();
}


Handle<Value> HyperClient::get(const Arguments& args)
{
/*          Handle<Value> client_obj = async_get(args);
    Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
    HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
*/
    return execute_op(args, GET);
}


Handle<Value> HyperClient::put(const Arguments& args)
{
    /*Handle<Value> client_obj = async_put(args);
    Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        Handle<Value> ret_val = obj->wait();
    free(obj);
    return ret_val;*/
    return execute_op(args, PUT);
}

Handle<Value> HyperClient::execute_op(const Arguments& args, enum Node_Op op)
{
    Handle<Value> client_obj;
    switch(op)
    {
        case PUT:
            client_obj = async_put(args);
            break;
        case GET:
            client_obj = async_get(args);
            break;
        case ATOMIC_ADD:
            client_obj = async_atomic_add(args);
                        break;
                case ATOMIC_SUB:
            client_obj = async_atomic_sub(args);
                        break;
                case ATOMIC_MUL:
            client_obj = async_atomic_mul(args);
                        break;
                case ATOMIC_DIV:
            client_obj = async_atomic_div(args);
                        break;
                case ATOMIC_MOD:
            client_obj = async_atomic_mod(args);
                        break;
                case ATOMIC_AND:
            client_obj = async_atomic_and(args);
                        break;
                case ATOMIC_OR:
            client_obj = async_atomic_or(args);
                        break;
                case ATOMIC_XOR:
            client_obj = async_atomic_xor(args);
                        break;
                case STRING_PREPEND:
            client_obj = async_string_prepend(args);
                        break;
                case STRING_APPEND:
            client_obj = async_string_append(args);
                        break;
                case LIST_LPUSH:
            client_obj = async_list_lpush(args);
                        break;
                case LIST_RPUSH:
            client_obj = async_list_rpush(args);
                        break;
        case SET_ADD:
            client_obj = async_set_add(args);
            break;
        case SET_REMOVE:
            client_obj = async_set_remove(args);
                        break;
        case SET_UNION:
            client_obj = async_set_union(args);
                        break;
        case SET_INTERSECT:
            client_obj = async_set_intersect(args);
                        break;
        default:
            exception("Unknown Operation");
            return v8::Undefined();
            break;
    }

        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}

Handle<Value> HyperClient::execute_map_op(const Arguments& args, enum Node_Op op)
{
        Handle<Value> client_obj;
        switch(op)
        {
                case MAP_ADD:
                        client_obj = async_map_add(args);
                        break;
                case MAP_REMOVE:
                        client_obj = async_map_remove(args);
                        break;
                case MAP_ATOMIC_ADD:
                        client_obj = async_map_atomic_add(args);
                        break;
                case MAP_ATOMIC_SUB:
                        client_obj = async_map_atomic_sub(args);
                        break;
                case MAP_ATOMIC_MUL:
                        client_obj = async_map_atomic_mul(args);
                        break;
                case MAP_ATOMIC_DIV:
                        client_obj = async_map_atomic_div(args);
                        break;
                case MAP_ATOMIC_MOD:
                        client_obj = async_map_atomic_mod(args);
                        break;
                case MAP_ATOMIC_AND:
                        client_obj = async_map_atomic_and(args);
                        break;
                case MAP_ATOMIC_OR:
                        client_obj = async_map_atomic_or(args);
                        break;
                case MAP_ATOMIC_XOR:
                        client_obj = async_map_atomic_xor(args);
                        break;
                case MAP_STRING_PREPEND:
                        client_obj = async_map_string_prepend(args);
                        break;
                case MAP_STRING_APPEND:
                        client_obj = async_map_string_append(args);
                        break;
        default:
            exception("Unknown Operation");
            break;
    }
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}

Handle<Value> HyperClient::async_execute_op(const Arguments& args, enum Node_Op op)
{
    int64_t val;
        HyperClient *obj, *obj1;
        std::string sp, ke;
        char *space, *key;
        size_t key_size, a_sz;
        struct hyperclient_attribute* a;
        Local<Array> arr, arr1;
        Local<Integer> temp, temp1;
        Local<Object> obj2;
        if(args.Length() != 3)
        {
                exception("Wrong Number of Arguments");
                return v8::Undefined();
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
//      printf("Space : %s Key : %s\n",space, key);
        key_size = strlen(key);
        obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);
        arr = Local<Array>::Cast(args[2]);
        arr1 = arr->GetPropertyNames();
        a_sz = (size_t)arr1->Length();
        a = (struct hyperclient_attribute *) malloc(a_sz*sizeof(struct hyperclient_attribute));
        dict_to_attrs(arr, 1, &a);
    switch(op)
    {
        case PUT:
                obj->val = hyperclient_put(obj->client, space, key, key_size, a, a_sz, obj->ret);
            break;
        case ATOMIC_ADD:
            obj->val = hyperclient_atomic_add(obj->client, space, key, key_size, a, a_sz, obj->ret);
            break;
        case ATOMIC_SUB:
            obj->val = hyperclient_atomic_sub(obj->client, space, key, key_size, a, a_sz, obj->ret);
            break;
        case ATOMIC_MUL:
            obj->val = hyperclient_atomic_mul(obj->client, space, key, key_size, a, a_sz, obj->ret);
            break;
                case ATOMIC_DIV:
                        obj->val = hyperclient_atomic_div(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case ATOMIC_MOD:
                        obj->val = hyperclient_atomic_mod(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case ATOMIC_AND:
                        obj->val = hyperclient_atomic_and(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case ATOMIC_OR:
                        obj->val = hyperclient_atomic_or(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case ATOMIC_XOR:
                        obj->val = hyperclient_atomic_xor(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case STRING_PREPEND:
                        obj->val = hyperclient_string_prepend(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case STRING_APPEND:
                        obj->val = hyperclient_string_append(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case LIST_LPUSH:
                        obj->val = hyperclient_list_lpush(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case LIST_RPUSH:
                        obj->val = hyperclient_list_rpush(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        case SET_ADD:
            obj->val = hyperclient_set_add(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        case SET_REMOVE:
            obj->val = hyperclient_set_remove(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        case SET_UNION:
            obj->val = hyperclient_set_union(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        case SET_INTERSECT:
            obj->val = hyperclient_set_intersect(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        default:
            exception("Invalid OP");
            return v8::Undefined();
    }

        if(obj->val < 0)
        {
                destroy_attrs(&a, a_sz);
                hyperclient_destroy_attrs(a, a_sz);
                raise_exception(*(obj->ret));
                return v8::Undefined();
        }
        obj->op = 2;
        ins_ops(obj->val, obj->op);
        destroy_attrs(&a, a_sz);
        hyperclient_destroy_attrs(a, a_sz);
        obj->Wrap(obj2);
        return obj2;

}

Handle<Value> HyperClient::async_execute_map_op(const Arguments& args, enum Node_Op op)
{
        int64_t val;
        HyperClient *obj, *obj1;
        std::string sp, ke;
        char *space, *key;
        size_t key_size, a_sz;
        struct hyperclient_map_attribute* a;
        Local<Integer> temp, temp1;
        Local<Object> obj2;
    Local<Object> arg_obj;
        if(args.Length() != 3)
        {
                exception("Wrong Number of Arguments");
                return v8::Undefined();
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
        key_size = strlen(key);
        obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);
    arg_obj = Local<Object>::Cast(args[2]);
        dict_to_map_attrs(arg_obj, &a, &a_sz);
        switch(op)
        {
                case MAP_ADD:
                        obj->val = hyperclient_map_add(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
        case MAP_REMOVE:
                        obj->val = hyperclient_map_remove(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_ADD:
                        obj->val = hyperclient_map_atomic_add(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_SUB:
                        obj->val = hyperclient_map_atomic_sub(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_MUL:
                        obj->val = hyperclient_map_atomic_mul(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_DIV:
                        obj->val = hyperclient_map_atomic_div(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_MOD:
                        obj->val = hyperclient_map_atomic_mod(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_AND:
                        obj->val = hyperclient_map_atomic_and(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_OR:
                        obj->val = hyperclient_map_atomic_or(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_ATOMIC_XOR:
                        obj->val = hyperclient_map_atomic_xor(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
/*                case MAP_STRING_PREPEND:
                        obj->val = hyperclient_map_string_prepend(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;
                case MAP_STRING_APPEND:
                        obj->val = hyperclient_map_string_append(obj->client, space, key, key_size, a, a_sz, obj->ret);
                        break;*/
        defualt:
            exception("Unknown Operation");
            break;
    }
    if(obj->val < 0)
        {
//                destroy_attrs(&a, a_sz);
//                hyperclient_destroy_attrs(a, a_sz);
                raise_exception(*(obj->ret));
                return v8::Undefined();
        }
        obj->op = 2;
        ins_ops(obj->val, obj->op);
//        destroy_attrs(&a, a_sz);
//        hyperclient_destroy_attrs(a, a_sz);
        obj->Wrap(obj2);
        return obj2;
}

#define HC_EXEC_OP(lower, UPPER) \
    Handle<Value> HyperClient:: lower (const Arguments& args) \
    { \
        return execute_op(args, UPPER); \
    } \
    Handle<Value> HyperClient:: async_ ## lower (const Arguments& args) \
    { \
        return async_execute_op(args, UPPER); \
    }

HC_EXEC_OP(atomic_add, ATOMIC_ADD)
HC_EXEC_OP(atomic_sub, ATOMIC_SUB)
HC_EXEC_OP(atomic_mul, ATOMIC_MUL)
HC_EXEC_OP(atomic_div, ATOMIC_DIV)
HC_EXEC_OP(atomic_mod, ATOMIC_MOD)
HC_EXEC_OP(atomic_and, ATOMIC_AND)
HC_EXEC_OP(atomic_or, ATOMIC_OR)
HC_EXEC_OP(atomic_xor, ATOMIC_XOR)
HC_EXEC_OP(string_prepend, STRING_PREPEND)
HC_EXEC_OP(string_append, STRING_APPEND)
HC_EXEC_OP(list_lpush, LIST_LPUSH)
HC_EXEC_OP(list_rpush, LIST_RPUSH)
HC_EXEC_OP(set_add, SET_ADD);
HC_EXEC_OP(set_remove, SET_REMOVE)
HC_EXEC_OP(set_union, SET_UNION)
HC_EXEC_OP(set_intersect, SET_INTERSECT)

#define HC_EXEC_MAP_OP(lower, UPPER) \
    Handle<Value> HyperClient:: lower (const Arguments& args) \
    { \
        return execute_map_op(args, UPPER); \
    } \
    Handle<Value> HyperClient:: async_ ## lower (const Arguments& args) \
    { \
        return async_execute_map_op(args, UPPER); \
    }

HC_EXEC_MAP_OP(map_add, MAP_ADD)
HC_EXEC_MAP_OP(map_remove, MAP_REMOVE)
HC_EXEC_MAP_OP(map_atomic_add, MAP_ATOMIC_ADD)
HC_EXEC_MAP_OP(map_atomic_sub, MAP_ATOMIC_SUB)
HC_EXEC_MAP_OP(map_atomic_mul, MAP_ATOMIC_MUL)
HC_EXEC_MAP_OP(map_atomic_div, MAP_ATOMIC_DIV)
HC_EXEC_MAP_OP(map_atomic_mod, MAP_ATOMIC_MOD)
HC_EXEC_MAP_OP(map_atomic_and, MAP_ATOMIC_AND)
HC_EXEC_MAP_OP(map_atomic_or, MAP_ATOMIC_OR)
HC_EXEC_MAP_OP(map_atomic_xor, MAP_ATOMIC_XOR)
HC_EXEC_MAP_OP(map_string_prepend, MAP_STRING_PREPEND)
HC_EXEC_MAP_OP(map_string_append, MAP_STRING_APPEND)
