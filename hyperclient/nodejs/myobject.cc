#define BUILDING_NODE_EXTENSION
#include <node.h>
#include "myobject.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <map>
#include "/home/ashik/node/cvv8/convert.hpp"
#include "hyperclient/hyperclient.h"
//namespace cv = ::v8::juice::convert;
using namespace v8;


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
Local<Array> myarr;
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

	myarr = Array::New();
  	constructor = Persistent<Function>::New(tpl->GetFunction());
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


void exception(const char* str)
{
	ThrowException(Exception::Error(newstr(str)));
}

Handle<Value> HyperClient::New(const Arguments& args) {
	if(args.Length() != 2)
	{
		exception("Wrong Arguments");
		return v8::Undefined();
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
//	printf("Length : %d\n", arr->Length());
	for(i = 0; i<arr->Length(); i++)
	{
		Local<Value> key = arr->Get(i);
                Local<Value> val = obj->Get(key);
                if(key->IsString())
                {
			(*a)[i].attr = getstr(key);
//			printf("%s : ",(*a)[i].attr);
                }
                else
                {
			exception("Invalid key");
                }

                if(val->IsString())
                {
			(*a)[i].value = getstr(val);
//			printf("%s\n",(*a)[i].value);
			(*a)[i].value_sz = strlen((*a)[i].value);
			(*a)[i].datatype = HYPERDATATYPE_STRING;
                }
                else
                {
                	num = cvv8::CastFromJS<int64_t>(val->ToInteger());
			if(!(isinc))
				num = -num;
//			printf("%ld\n",num);
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
	const char* str;
	int64_t num;
	for(i=0;i<attrs_size;i++)
	{
		key = newstr((char *)attrs[i].attr);
		if(attrs[i].datatype == HYPERDATATYPE_STRING)
		{
			if(attrs[i].value_sz != 0)
			{
				str = (const char *)copy_n(attrs[i].value, attrs[i].value_sz);
				val = newstr(str);
			}
			else
			{
				str = (const char *)"";
				val = newstr(str);
			}
//			printf("%s\n", str);
		}
		else if(attrs[i].datatype == HYPERDATATYPE_INT64)
		{
			if(attrs[i].value_sz != 0)
			{
				memmove(&num, attrs[i].value, attrs[i].value_sz);
                                num = le64toh(num);		
//				printf("%ld\n", num);
				val = Integer::New(num);
			}
			else
			{
				val = Integer::New(0);
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
	hc_list = get_attrs(obj->val);
	op = get_ops(obj->val);
	if(hc_list != NULL)
	{
//		printf("Val = %ld, OP = %d, list size = %zd\n", obj->val, op, hc_list->size());
		if(hc_list->size() != 0)
		{		
			attr = hc_list->front();
			hc_list->pop_front();
			ins_attrs(obj->val, hc_list);
			if(attr->isAttr == true)
			{
				return attrs_to_dict(attr->attrs, attr->attrs_size);
			}
			else
			{
				return Boolean::New(attr->retval);
			}
		}
		else if(op == 0)
		{
			map_attrs.erase(obj->val);
			return v8::Undefined();
		}
	}
	do
	{
		val = hyperclient_loop(obj->client, -1, obj->ret);
		if(*(obj->ret) != HYPERCLIENT_SUCCESS && *(obj->ret) != HYPERCLIENT_NOTFOUND && *(obj->ret) != HYPERCLIENT_CMPFAIL && *(obj->ret) != HYPERCLIENT_SEARCHDONE)
		{
			exception("Hyperdex error");
			return v8::Undefined();
		}
		hc_list = get_attrs(val);
		if(hc_list == NULL)
		{
			hc_list = new std::list<struct hc_attrs*>();
		}
		op = get_ops(val);
//		printf("list size = %zd, Val = %ld, OP = %d\n",hc_list->size(), val, op);
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
		map_attrs.erase(obj->val);
	}
	if(attr->isAttr == true)
        {
	        return attrs_to_dict(attr->attrs, attr->attrs_size);
        }
        else
        {
	       	return Boolean::New(attr->retval);
        }
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
        printf("Space : %s Key : %s\n",space, key);
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
		hyperclient_destroy_attrs(a, a_sz);
		hyperclient_destroy_attrs(b, b_sz);
                exception("CondPut Failed");
                return v8::Undefined();
	}
	obj->op = 3;
	ins_ops(obj->val, obj->op);
    	hyperclient_destroy_attrs(a, a_sz);
    	hyperclient_destroy_attrs(b, b_sz);
	obj->Wrap(obj2);
	return obj2;
}

Handle<Value> HyperClient::async_atomicinc(const Arguments& args)
{
	std::map<int64_t, std::list<struct hc_attrs* > * > test;
	int64_t t1;
//	std::list<struct hc_attrs*> t2;
	std::list<struct hc_attrs*> *t;
	t1 = 1;
	t = test[t1];
	if(t == NULL)
		t = new std::list<struct hc_attrs*>();
	test.erase(t1);
	test.insert(std::pair<int64_t, std::list<struct hc_attrs* > * >(t1, t));
	if(test[t1] == NULL)
	{
		printf("This is Crazy\n");
	}
	else
	{
		printf("Thank God!\n");
        }
	return v8::Undefined();
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
                exception("Get Failed");
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
                        exception("Invalid key");
                }

		v1 = v_arr->Get(0);
                v2 = v_arr->Get(1);
		
		if(!v1->IsNumber() || !v2->IsNumber())
		{
			exception("Wrong Datatype\n");
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
		hyperclient_destroy_attrs(a, a_sz);
		free(r);
		exception("Search Failed");	
		return v8::Undefined();
	}
	obj->op = 7;
	ins_ops(obj->val, obj->op);
	hyperclient_destroy_attrs(a, a_sz);
	free(r);
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
//	printf("Space : %s Key : %s\n",space, key);
	key_size = strlen(key);
	obj2 = args.This()->Clone();
        obj = ObjectWrap::Unwrap<HyperClient>(obj2);
        obj1 = ObjectWrap::Unwrap<HyperClient>(args.This());
        copy_client(&obj, &obj1);

	obj->val = hyperclient_get(obj->client, space, key, key_size, obj->ret, obj->attrs, obj->attrs_size);
	if(obj->val < 0)
	{
		exception("Get Failed");
		return v8::Undefined();
	}
	obj->op = 1;	
	ins_ops(obj->val, obj->op);
	obj->Wrap(obj2);
	return obj2;
}


Handle<Value> HyperClient::async_put(const Arguments& args)
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
//	printf("Space : %s Key : %s\n",space, key);
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
	obj->val = hyperclient_put(obj->client, space, key, key_size, a, a_sz, obj->ret);
	if(obj->val < 0)
	{
		hyperclient_destroy_attrs(a, a_sz);
		exception("Put Failed");
		return v8::Undefined();
	}
	obj->op = 2;
	ins_ops(obj->val, obj->op);
	hyperclient_destroy_attrs(a, a_sz);
	obj->Wrap(obj2);
	return obj2;
}


Handle<Value> HyperClient::condput(const Arguments& args)
{
	Handle<Value> client_obj = async_condput(args);
        Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
}

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
	HandleScope scope;
        return scope.Close(Undefined());
}


Handle<Value> HyperClient::get(const Arguments& args)
{
       	Handle<Value> client_obj = async_get(args);
	Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
	HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
       	return obj->wait();
}


Handle<Value> HyperClient::put(const Arguments& args)
{	
	Handle<Value> client_obj = async_put(args);
	Handle<Object> obj1 = Handle<Object>::Cast(client_obj);
        HyperClient *obj = ObjectWrap::Unwrap<HyperClient>(obj1);
        return obj->wait();
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




