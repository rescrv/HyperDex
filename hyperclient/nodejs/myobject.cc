#define BUILDING_NODE_EXTENSION
#include <node.h>
#include "myobject.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include "/home/ashik/node/cvv8/convert.hpp"
#include "hyperclient/hyperclient.h"
//namespace cv = ::v8::juice::convert;
using namespace v8;


HyperClient::HyperClient() {};
HyperClient::~HyperClient() {};

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
	tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomicinc"), FunctionTemplate::New(async_atomicinc)->GetFunction());
        tpl->PrototypeTemplate()->Set(String::NewSymbol("atomicinc"), FunctionTemplate::New(atomicinc)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("async_atomicdec"), FunctionTemplate::New(async_atomicdec)->GetFunction());
        tpl->PrototypeTemplate()->Set(String::NewSymbol("atomicdec"), FunctionTemplate::New(atomicdec)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("async_search"), FunctionTemplate::New(async_search)->GetFunction());
        tpl->PrototypeTemplate()->Set(String::NewSymbol("search"), FunctionTemplate::New(search)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("async_delete"), FunctionTemplate::New(async_del)->GetFunction());
        tpl->PrototypeTemplate()->Set(String::NewSymbol("delete"), FunctionTemplate::New(del)->GetFunction());
	
  	constructor = Persistent<Function>::New(tpl->GetFunction());
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

Handle<Value> HyperClient::New(const Arguments& args) {
  	HandleScope scope;
	if(args.Length() != 2)
	{
		exception("Wrong Arguments");
		return scope.Close(Undefined());
	}
  	HyperClient* obj = new HyperClient();
//  	obj->val_ = args[0]->IsUndefined() ? 0 : args[0]->NumberValue();
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


void dict_to_attrs(Local<Object> obj, int isinc, struct hyperclient_attribute** a)
{
	int i;
	int64_t num;
	Local<Value> key, val;
	Local<Array> arr = obj->GetPropertyNames();
	printf("Length : %d\n", arr->Length());
	for(i = 0; i<arr->Length(); i++)
	{
		Local<Value> key = arr->Get(i);
                Local<Value> val = obj->Get(key);
                if(key->IsString())
                {
			(*a)[i].attr = getstr(key);
			printf("%s : ",(*a)[i].attr);
                }
                else
                {
			exception("Invalid key");
                }

                if(val->IsString())
                {
			(*a)[i].value = getstr(val);
			printf("%s\n",(*a)[i].value);
			(*a)[i].value_sz = strlen((*a)[i].value);
			(*a)[i].datatype = HYPERDATATYPE_STRING;
                }
                else
                {
                	num = cvv8::CastFromJS<int64_t>(val->ToInteger());
			if(!(isinc))
				num = -num;
			printf("%ld\n",num);
			num = htole64(num);
			(*a)[i].value = (char *)malloc(8*sizeof(char));
			memmove((void*)(*a)[i].value, &num, (size_t)8);
			(*a)[i].value_sz = 8;
			(*a)[i].datatype = HYPERDATATYPE_INT64;
                }	
	}
	return;
}


Local<Array> attrs_to_dict(struct hyperclient_attribute* attrs, size_t attrs_size)
{
	int i;
	Local<Array> obj = Array::New();
	Local<Value> key, val;
	int64_t num;
	for(i=0;i<attrs_size;i++)
	{
		key = newstr((char *)attrs[i].attr);
		if(attrs[i].datatype == HYPERDATATYPE_STRING)
		{
			val = newstr((char *)attrs[i].value);
			printf("%s\n", attrs[i].value);
		}
		else if(attrs[i].datatype == HYPERDATATYPE_INT64)
		{
			if(attrs[i].value_sz != 0)
			{
				memmove(&num, attrs[i].value, attrs[i].value_sz);
                                num = le64toh(num);		
				printf("%ld\n", num);
				val = Integer::New(num);
			}
			 
		}
		else
		{
			Local<Array> nullarr;
			//Wrong type
			exception("Wrong Type");
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
	HandleScope scope;
	HyperClient* obj = ObjectWrap::Unwrap<HyperClient>(args.This());
	int64_t val;
	Local<Value>  op, attr_val, new_op, new_attrs;	
	Local<Array> arr, attrs;
	attrs = Local<Array>::Cast(obj->result->Get(obj->val));
	op = obj->ops->Get(obj->val);
	if(op->IsUndefined())
	{	
		scope.Close(Undefined());
	}
	if(!attrs->IsUndefined())
	{		
		attr_val = arr_shift(attrs);
		if(!attr_val->IsUndefined())
		{
			obj->result->Set(obj->val, attrs);
			return scope.Close(attr_val);
		}
		else if(toint64(op) == 0)
		{
			obj->result->Delete(obj->val);
			obj->ops->Delete(obj->val);
			return scope.Close(Undefined());
		}
	}
	do
	{
////		val = hyperclient_loop(obj->client, -1, obj->ret);
		if(*(obj->ret) != HYPERCLIENT_SUCCESS && *(obj->ret) != HYPERCLIENT_NOTFOUND && *(obj->ret) != HYPERCLIENT_CMPFAIL && *(obj->ret) != HYPERCLIENT_SEARCHDONE)
		{
			exception("Hyperdex error");
			return scope.Close(Undefined());
		}
		arr = Local<Array>::Cast(obj->result->Get(val));
	        op = obj->ops->Get(val);
		if(arr->IsUndefined())
		{
			arr = Array::New();
		}
		int arr_len = arr->Length();
		if(toint64(new_op) == 1)
		{
			if(*(obj->ret) == HYPERCLIENT_SUCCESS)
			{
				obj->ops->Set(val, Integer::New(0));
				new_attrs = Local<Array>::Cast(attrs_to_dict(*(obj->attrs), *(obj->attrs_size)));
				arr_unshift(arr, new_attrs);
			}
			else
			{
				obj->ops->Set(val, Integer::New(0));
				arr_unshift(arr, newstr((char*)"False"));
			}
			obj->result->Set(val, arr);
		}
		else
		{
			if(*(obj->ret) == HYPERCLIENT_SUCCESS)
			{
				obj->ops->Set(val, Integer::New(0));
				arr_unshift(arr, newstr((char *)"True"));
			}
			else
			{
				obj->ops->Set(val, Integer::New(0));
				arr_unshift(arr, newstr((char *)"False"));
			}
			obj->result->Set(val, arr);
		}
	}while(val != obj->val);
	attr_val = arr_shift(arr);
	obj->result->Set(val, arr);
	new_op = obj->ops->Get(val);
	if(toint64(new_op) == 0)
	{
		obj->result->Delete(val);
		obj->ops->Delete(val);
	}
	return attr_val;
}


Handle<Value> HyperClient::async_condput(const Arguments& args)
{
	HandleScope scope;
	HyperClient* obj;
	char *space, *key;
	std::string sp, ke;
	size_t key_size, a_sz, b_sz;
	struct hyperclient_attribute *a, *b;
	Local<Array> arr, arr1, arr2, arr3;
	if(args.Length() != 4)
        {
                exception("Wrong Number of Arguments");
                return scope.Close(Undefined());
        }
	sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
        printf("Space : %s Key : %s\n",space, key);
        key_size = strlen(key);
	obj = ObjectWrap::Unwrap<HyperClient>(args.This());
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
		////            hyperclient_destroy_attrs(a, a_sz);
		////            hyperclient_destroy_attrs(b, b_sz);
                exception("CondPut Failed");
                return scope.Close(Undefined());
	}
	obj->op = 3;
        obj->ops->Set(obj->val, Integer::New(3));
////    hyperclient_destroy_attrs(a, a_sz);
////    hyperclient_destroy_attrs(b, b_sz);
	return args.This();
}

Handle<Value> HyperClient::async_atomicinc(const Arguments& args)
{
	HandleScope scope;
	//Local<Value> obj = Object::New();
	if(args[0]->IsObject())
	{
		printf("It is an Object\n");
	}
	if(args[0]->IsArray())
	{
		printf("It is an Array\n");
	}
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::async_atomicdec(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::async_del(const Arguments& args)
{
	HandleScope scope;
        std::string sp, ke;
        char *space, *key;
        size_t key_size;
        HyperClient *obj;
        if(args.Length() != 2)
        {
                exception("Wrong Number of Arguments");
                return scope.Close(Undefined());
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
        printf("Space : %s Key : %s\n",space, key);
        key_size = strlen(key);
        obj = ObjectWrap::Unwrap<HyperClient>(args.This());
////    obj->val = hyperclient_del(obj->client, space, key, key_size, obj->ret);
        obj->val = 1;
        if(obj->val < 0)
        {
                exception("Get Failed");
                return scope.Close(Undefined());
        }
        obj->op = 4;
        obj->ops->Set(obj->val, Integer::New(1));
        return args.This();
}

Handle<Value> HyperClient::async_search(const Arguments& args)
{
	HandleScope scope;
	std::string sp, ke;
        char *space;
        size_t sz, a_sz, r_sz;
        HyperClient *obj;
	Local<Array> arr, arr1, attr_arr, range_arr;
	Local<Value> k, v;
	struct hyperclient_attribute* a;
	struct hyperclient_range_query* r;
	int i;
        if(args.Length() != 2)
        {
                exception("Wrong Number of Arguments");
                return scope.Close(Undefined());
        }
        sp = tostr(args[0]);
        space = (char *) sp.c_str();
        printf("Space : %s\n",space);
	obj = ObjectWrap::Unwrap<HyperClient>(args.This());
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
	dict_to_attrs(attr_arr, 1, &a);
	
	
        return scope.Close(Undefined());
}


Handle<Value> HyperClient::async_get(const Arguments& args)
{
	HandleScope scope;
	std::string sp, ke;
	char *space, *key;
	size_t key_size;
	HyperClient *obj;
	if(args.Length() != 2)
	{
		exception("Wrong Number of Arguments");
		return scope.Close(Undefined());
	}
	sp = tostr(args[0]);
	space = (char *) sp.c_str();
	ke = tostr(args[1]);
	key = (char *) ke.c_str();
	printf("Space : %s Key : %s\n",space, key);
	key_size = strlen(key);
	obj = ObjectWrap::Unwrap<HyperClient>(args.This());
////	obj->val = hyperclient_get(obj->client, space, key, key_size, obj->ret, obj->attrs, obj->attrs_size);
	obj->val = 1;
	if(obj->val < 0)
	{
		exception("Get Failed");
		return scope.Close(Undefined());
	}
	obj->op = 1;	
	obj->ops->Set(obj->val, Integer::New(1));
	return args.This();
}


Handle<Value> HyperClient::async_put(const Arguments& args)
{
	HandleScope scope;
	int64_t val;
	HyperClient* obj;
	std::string sp, ke;
	char *space, *key;
	size_t key_size, a_sz;
	struct hyperclient_attribute* a;
	Local<Array> arr, arr1;
	Local<Value> temp, temp1;
	if(args.Length() != 3)
	{
		exception("Wrong Number of Arguments");
		return scope.Close(Undefined());
	}
	sp = tostr(args[0]);
        space = (char *) sp.c_str();
        ke = tostr(args[1]);
        key = (char *) ke.c_str();
	printf("Space : %s Key : %s\n",space, key);
	key_size = strlen(key);
	obj = ObjectWrap::Unwrap<HyperClient>(args.This());
	arr = Local<Array>::Cast(args[2]);
	arr1 = arr->GetPropertyNames();
	a_sz = (size_t)arr1->Length();
	a = (struct hyperclient_attribute *) malloc(a_sz*sizeof(struct hyperclient_attribute));
	dict_to_attrs(arr, 1, &a);
	obj->val = hyperclient_put(obj->client, space, key, key_size, a, a_sz, obj->ret);
	obj->val = 1;
	if(obj->val < 0)
	{
		hyperclient_destroy_attrs(a, a_sz);
		exception("Put Failed");
		return scope.Close(Undefined());
	}
	val = hyperclient_loop(obj->client, -1, obj->ret);
	if(val != obj->val || *(obj->ret) != HYPERCLIENT_SUCCESS)
	{
		printf("Some problem");
	}
	printf("Put worked");
	hyperclient_destroy_attrs(a, a_sz);
	return args.This();
}


Handle<Value> HyperClient::condput(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::atomicinc(const Arguments& args)
{
	HandleScope scope;
	
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::atomicdec(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::del(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::search(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}

Handle<Value> HyperClient::destroy(const Arguments& args)
{
	HandleScope scope;
        return scope.Close(Undefined());
}


Handle<Value> HyperClient::get(const Arguments& args)
{
        HandleScope scope;

	if(args.Length() != 2)
	{
		exception("Wrong number of Arguments");
                return scope.Close(Undefined());
	}
	else
	{
        	Handle<Value> client_obj = async_get(args);
        	return wait(args);
	}
}


Handle<Value> HyperClient::put(const Arguments& args)
{	
	HandleScope scope;
	
	if(args.Length() != 3)
	{
		exception("Wrong number of Arguments");
		return scope.Close(Undefined());
	}
	else
	{
		Handle<Value> obj = async_put(args);
		return wait(args);
	}
}

/*
                Local<Object> obj = args[0]->ToObject();
                dict_to_attrs(obj);
                return scope.Close(Undefined());
*/

/*      Local<Array> arr(Array::New());
        arr->Set(String::New("one"),Integer::New(1));
        return scope.Close(arr);

*/

//HyperClient* obj = ObjectWrap::Unwrap<HyperClient>(args.This());

//      printf("a : %d\n", obj->st->a);
//      return scope.Close(Number::New(obj->st->a));
//      return scope.Close(True());




