#include "hyperclient/hyperclient.h"
#include "ruby.h"
#include <stdio.h>
#include <endian.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>


struct ruby_hyperclient{
        struct hyperclient* client;
        struct hyperclient_attribute** attrs;
        size_t *attrs_size;
        enum hyperclient_returncode* ret;
        int finished;
        int op;
	int64_t val;
};

VALUE hash = Qnil;
VALUE hash_op = Qnil;
VALUE chyperclient;


void ruby_exception(char* exception)
{
        rb_raise(rb_eArgError, exception);
}

void raise(enum hyperclient_returncode ret)
{
	switch(ret)
	{

                case HYPERCLIENT_NOTFOUND: 
			ruby_exception("Not Found");
			break;
		case HYPERCLIENT_SEARCHDONE: 
			ruby_exception("Search Done");
			break;
		case HYPERCLIENT_CMPFAIL: 
			ruby_exception("Conditional Operation Did Not Match Object");
			break;
		case HYPERCLIENT_UNKNOWNSPACE: 
			ruby_exception("Unknown Space");
			break;
		case HYPERCLIENT_COORDFAIL: 
			ruby_exception("Coordinator Failure");
			break;	
                case HYPERCLIENT_SERVERERROR: 
			ruby_exception("Server Error");
			break;
                case HYPERCLIENT_CONNECTFAIL: 
			ruby_exception("Connection Failure");
			break;
                case HYPERCLIENT_DISCONNECT: 
			ruby_exception("Connection Reset");
			break;
                case HYPERCLIENT_RECONFIGURE: 
			ruby_exception("Reconfiguration");
			break;
                case HYPERCLIENT_LOGICERROR: 
			ruby_exception("Logic Error (file a bug)");
			break;
                case HYPERCLIENT_TIMEOUT:
			ruby_exception("Timeout");
			break;
                case HYPERCLIENT_UNKNOWNATTR: 
			ruby_exception("Unknown attribute");
			break;
                case HYPERCLIENT_DUPEATTR: 
			ruby_exception("Duplicate attribute ");
			break;
                case HYPERCLIENT_SEEERRNO: 
			ruby_exception("See ERRNO");
			break;
                case HYPERCLIENT_NONEPENDING: 
			ruby_exception("None pending");
			break;
                case HYPERCLIENT_DONTUSEKEY: 
			ruby_exception("Do not specify the key in a search predicate and do not redundantly specify the key for an insert");
			break;
                case HYPERCLIENT_WRONGTYPE: 
			ruby_exception("Attribute has the wrong type");
			break;
                case HYPERCLIENT_EXCEPTION: 
			ruby_exception("Internal Error (file a bug)");
			break;
	}
}


void destroy_attrs(struct hyperclient_attribute** a, size_t size)
{
	int i;
	for(i=0;i<size;i++)
	{
		free((void *)(*a)[i].value);
	}
	return;
}


void hash_insert(VALUE* hash, VALUE key, VALUE val)
{
        if(*hash == Qnil)
        {
                *hash = rb_hash_new();
	}
        rb_hash_aset(*hash, key, val);
	
}

struct ruby_hyperclient* copy_client(VALUE self)
{
	struct ruby_hyperclient *r, *r1;
	Data_Get_Struct(self, struct ruby_hyperclient, r);
	r1 = ALLOC(struct ruby_hyperclient);
	if(r1 == NULL)
	{
		rb_raise(rb_eArgError,"Memory Error");
		return NULL;
	}
	memcpy(r1, r, sizeof(struct ruby_hyperclient));
	if(r1->client == NULL)
	{
		rb_raise(rb_eArgError,"Client Error");
	}
	return r1;
}

VALUE i64tonum(int64_t val)
{
	return LL2NUM((long long)val);
}

int ary_len(VALUE array)
{
	VALUE a, b;
	int i = 0;
	a = rb_ary_new();
	while((b=rb_ary_pop(array)) != Qnil)
	{
		i++;
		rb_ary_push(b,a);
	}
	while((b=rb_ary_pop(a)) != Qnil)
	{
		rb_ary_push(b,array);
	}
	return i;
}	

char* copy_n(const char* str, int n)
{
        int i;
        char * str1;
	str1 = ALLOC_N(char, n+1);
        for(i=0;i<n;i++)
                str1[i] = str[i];
        str1[i] = '\0';
        return str1;
}

VALUE attrs_to_array(struct hyperclient_attribute* attrs, size_t attrs_size)
{
        VALUE arr, Key, value, a;
        int i;
        int64_t num=0;
        char* strvalue;
        arr = rb_ary_new();
        for(i=0; i<attrs_size; i++)
        {
                a = rb_ary_new();
                Key = rb_str_new2(attrs[i].attr);
                rb_ary_push(a,Key);
                if(attrs[i].datatype == HYPERDATATYPE_STRING)
                {
			if(attrs[i].value!=NULL)
	                        strvalue = copy_n(attrs[i].value, attrs[i].value_sz);
                        value = rb_str_new2(strvalue);
			free(strvalue);
                }
                else if(attrs[i].datatype == HYPERDATATYPE_INT64)
                {
                        if(attrs[i].value!=NULL)
                        {
				if(attrs[i].value_sz == 0)
				{
					value = Qnil;
				}
				else
				{
                                	memmove(&num, attrs[i].value, attrs[i].value_sz);
                                	num = le64toh(num);
                                	value = i64tonum(num);
				}
                        }
                        else
                        {
                                value = Qnil;
                        }
                }
                else
                {
			hyperclient_destroy_attrs(attrs, attrs_size);
			raise(HYPERCLIENT_WRONGTYPE);
			return Qnil;
                }
                rb_ary_push(a,value);
                rb_ary_push(arr, a);
        }
	hyperclient_destroy_attrs(attrs, attrs_size);
        return arr;
}

void array_to_attrs(VALUE attrs, int isinc, struct hyperclient_attribute** a, size_t len )
{
        int i;
        int64_t num;
        VALUE key, value;

        for(i=0;i<len;i++)
        {
                key = RARRAY(RARRAY(attrs)->ptr[i])->ptr[0];
                value = RARRAY(RARRAY(attrs)->ptr[i])->ptr[1];
                (*a)[i].attr = STR2CSTR(key);
                if(FIXNUM_P(value))
                {
                        num = (int64_t)NUM2LL(value);
                        if(!(isinc))
                                num = -num;
                        num = htole64(num);
			(*a)[i].value  = ALLOC_N(char, 8);
                        memmove((void *)(*a)[i].value, &num, (size_t)8); 
			(*a)[i].value_sz = 8;
                        (*a)[i].datatype = HYPERDATATYPE_INT64;
	        }
                else
                {
                        (*a)[i].value = STR2CSTR(value);
                        (*a)[i].value_sz = strlen((*a)[i].value);
                        (*a)[i].datatype = HYPERDATATYPE_STRING;
                }
        }
        return;
}

VALUE hc_wait(VALUE self)
{
	int64_t val;
	struct ruby_hyperclient *r;
	VALUE attrs, attrs_array, new_attrs;
	VALUE op, new_op;
	VALUE attr_val;
	Data_Get_Struct(self, struct ruby_hyperclient, r);
	if(r == NULL || r->client == NULL)
	{
		rb_raise(rb_eArgError, "Client Error\n");
		return Qnil;
	}
	if(hash == Qnil)
	{
		return Qnil;
	}
	attrs = rb_hash_lookup(hash, i64tonum(r->val));
	op = rb_hash_lookup(hash_op, i64tonum(r->val));
	if(op == Qnil)
	{
		return Qnil;
	}
	if(attrs != Qnil)
	{
		attr_val = rb_ary_shift(attrs);
		if(attr_val!= Qnil)	
		{
			hash_insert(&hash, i64tonum(r->val), attrs);
			return attr_val;
		}
		else if(NUM2INT(op) == 0)
		{
			rb_hash_delete(hash_op, i64tonum(r->val));
			rb_hash_delete(hash, i64tonum(r->val));
			return Qnil;
		}
	}	
	do
	{
		val=hyperclient_loop(r->client, -1, r->ret);
		if(*(r->ret) != HYPERCLIENT_SUCCESS && *(r->ret) != HYPERCLIENT_NOTFOUND && *(r->ret) != HYPERCLIENT_CMPFAIL && *(r->ret) != HYPERCLIENT_SEARCHDONE)
		{
			raise(*(r->ret));
			return Qnil;
		}
		attrs_array = rb_hash_lookup(hash, i64tonum(val));
		new_op = rb_hash_lookup(hash_op, i64tonum(val));
		if(attrs_array == Qnil)
		{
			attrs_array = rb_ary_new();
		}
		if(NUM2INT(new_op) == 1)
		{	
			if(*(r->ret) == HYPERCLIENT_SUCCESS)
			{	
				hash_insert(&hash_op, i64tonum(val), INT2NUM(0));
				new_attrs = attrs_to_array(*(r->attrs), *(r->attrs_size));
				rb_ary_unshift(attrs_array, new_attrs);
			}
			else
			{
				rb_ary_unshift(attrs_array, Qfalse);
			}
			hash_insert(&hash, i64tonum(val), attrs_array);
			
		} 
		else if(NUM2INT(new_op) == 7)
		{
			if(*(r->ret) == HYPERCLIENT_SUCCESS)
			{
				new_attrs = attrs_to_array(*(r->attrs), *(r->attrs_size));
				rb_ary_unshift(attrs_array, new_attrs);	
			}
			else if(*(r->ret) == HYPERCLIENT_SEARCHDONE)
                        {
        	                hash_insert(&hash_op, i64tonum(r->val), INT2NUM(0));
                        }
			else
			{
				rb_ary_unshift(attrs_array, Qfalse);
			}
			hash_insert(&hash, i64tonum(val), attrs_array);
		}
		else
		{
			if(*(r->ret) == HYPERCLIENT_SUCCESS)
			{	
				hash_insert(&hash_op, i64tonum(val), INT2NUM(0));
				rb_ary_unshift(attrs_array, Qtrue);
			}
			else
			{
				rb_ary_unshift(attrs_array, Qfalse);
			}
			hash_insert(&hash, i64tonum(val), attrs_array);
		}
	}while(val != r->val);

	attr_val = rb_ary_shift(attrs_array);
	hash_insert(&hash, i64tonum(r->val), attrs_array);
	new_op = rb_hash_lookup(hash_op, i64tonum(r->val));
	if(NUM2INT(new_op) == 0)
	{
		rb_hash_delete(hash_op, i64tonum(val));
		rb_hash_delete(hash, i64tonum(val));
	}
	return attr_val;
}

VALUE hc_next(VALUE self)
{
	struct ruby_hyperclient* r;
	Data_Get_Struct(self, struct ruby_hyperclient, r);
	if(r == NULL || r->client == NULL)
	{
		return Qnil;
	}
	if(r->op != 7)
	{
		rb_raise(rb_eArgError, "Invalid operation");
		return Qnil;
	}
	return hc_wait(self);
}

VALUE hc_async_atomicincdec(VALUE self, int isinc, VALUE space, VALUE key, VALUE attrs)
{
	int64_t val;
        struct ruby_hyperclient *r;
        char* hc_space;
        char* hc_key;
        size_t hc_key_size;
        VALUE client;
        enum hyperclient_returncode code;
        struct hyperclient_attribute* a;
        size_t a_sz;
	int i;
        r = copy_client(self);
	hc_space = STR2CSTR(space);
        hc_key = STR2CSTR(key);
        hc_key_size = strlen(hc_key);
        a_sz = ary_len(attrs);
	a = ALLOC_N(struct hyperclient_attribute, a_sz);
 	array_to_attrs(attrs, isinc, &a, a_sz);
	
	for(i=0;i<a_sz;i++)
	{
		if(a[i].datatype != HYPERDATATYPE_INT64)
		{
			destroy_attrs(&a, a_sz);
		        hyperclient_destroy_attrs(a, a_sz);
			free(r);
			raise(HYPERCLIENT_WRONGTYPE);
			return Qnil;
		}
	}
	r->val = hyperclient_atomicinc(r->client, hc_space, hc_key, hc_key_size, a, a_sz, r->ret);
	if(r->val<0)
	{
		destroy_attrs(&a, a_sz);
	        hyperclient_destroy_attrs(a, a_sz);
		free(r);
		raise(*(r->ret));
		return Qnil;
	}
	r->op = 5;
        client = Data_Wrap_Struct(chyperclient, 0, 0, r);
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(5));
	destroy_attrs(&a, a_sz);
	hyperclient_destroy_attrs(a, a_sz);
	return client;
}

VALUE hc_async_atomicinc(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
	return hc_async_atomicincdec(self, 1, space, key, attrs);
}

VALUE hc_async_atomicdec(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
	return hc_async_atomicincdec(self, 0, space, key, attrs);
}

VALUE hc_async_condput(VALUE self, VALUE space, VALUE key, VALUE old_attrs, VALUE new_attrs)
{
        int64_t val;
        struct ruby_hyperclient *r;
	char* hc_space;
        char* hc_key;
        size_t hc_key_size;
        enum hyperclient_returncode code;
		
        struct hyperclient_attribute* a;
	struct hyperclient_attribute* b;
	
	size_t a_sz, b_sz;
	VALUE client;
	r = copy_client(self);
	hc_space = STR2CSTR(space);
        hc_key = STR2CSTR(key);
        hc_key_size = strlen(hc_key);
        a_sz = ary_len(old_attrs);
	b_sz = ary_len(new_attrs);
	a = ALLOC_N(struct hyperclient_attribute, a_sz);
	b = ALLOC_N(struct hyperclient_attribute, b_sz);
        array_to_attrs(old_attrs, 1, &a, a_sz);
	array_to_attrs(new_attrs, 1, &b, b_sz);
	r->val = hyperclient_condput(r->client, hc_space, hc_key, hc_key_size, a, a_sz, b, b_sz, r->ret);
	if(r->val < 0)
	{
		destroy_attrs(&a, a_sz);
	        destroy_attrs(&b, b_sz);
        	hyperclient_destroy_attrs(a,a_sz);
	        hyperclient_destroy_attrs(b,b_sz);
		free(r);
		raise(*(r->ret));
		return Qnil;
	}
	r->op = 3;	//Condput
	client = Data_Wrap_Struct(chyperclient, 0, 0, r);
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(r->op));
	destroy_attrs(&a, a_sz);
	destroy_attrs(&b, b_sz);
        hyperclient_destroy_attrs(a,a_sz);
	hyperclient_destroy_attrs(b,b_sz);
	return client;	
}

VALUE hc_async_get(VALUE self, VALUE space, VALUE key)
{
        struct ruby_hyperclient *r;
        char* hc_space;
        char* hc_key;
        size_t hc_key_size;
        int64_t val;
	VALUE client;
	VALUE attrs;
	VALUE arr, a, Key, value;
	int i;
	char *strvalue;
	r = copy_client(self);
	hc_space = STR2CSTR(space);
        hc_key = STR2CSTR(key);
        hc_key_size = strlen(hc_key);
	r->val = hyperclient_get(r->client, hc_space, hc_key, hc_key_size, r->ret, r->attrs, r->attrs_size);
	if(r->val<0)
	{
		free(r);
		raise(*(r->ret));
		return Qnil;
	}
	r->op = 1;
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(r->op));
	client = Data_Wrap_Struct(chyperclient, 0, 0, r);
	return client; 
}

VALUE hc_async_put(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
	int64_t val;
	struct ruby_hyperclient *r;
        char* hc_space;
        char* hc_key;
        size_t hc_key_size;
	int i;
	VALUE client;
	int64_t num;
	enum hyperclient_returncode code;	
	struct hyperclient_attribute* a;
        r = copy_client(self);
	hc_space = STR2CSTR(space);
        hc_key = STR2CSTR(key);
        hc_key_size = strlen(hc_key);
        *(r->attrs_size) = ary_len(attrs);
	a = ALLOC_N(struct hyperclient_attribute, *(r->attrs_size));
	array_to_attrs(attrs, 1, &a, *(r->attrs_size));
        r->val = hyperclient_put(r->client, hc_space, hc_key, hc_key_size, a, *(r->attrs_size), r->ret);
        if(r->val < 0)
        {
		destroy_attrs(&a, *(r->attrs_size));
               	hyperclient_destroy_attrs(a, *(r->attrs_size));
		free(r);
		raise(*(r->ret));
		return Qnil;
        }
	r->op = 2;
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(r->op));
	client = Data_Wrap_Struct(chyperclient, 0, 0, r);
	destroy_attrs(&a, *(r->attrs_size));
	hyperclient_destroy_attrs(a, *(r->attrs_size));
	return client;
}

VALUE hc_async_search(VALUE self, VALUE space, VALUE attrs)
{
	int64_t val, num;
        struct ruby_hyperclient * r;
        char* hc_space;
        enum hyperclient_returncode code;
        struct hyperclient_attribute* a;
        struct hyperclient_range_query* range;
        size_t sz, a_sz, r_sz;
        VALUE client, k, v, v1, v2;
	int i, ai, ri;

       	r = copy_client(self); 
        hc_space = STR2CSTR(space);
        sz = ary_len(attrs);

	a_sz = 0;
	r_sz = 0;
	for(i = 0; i < sz; i++)
	{
                v = RARRAY(RARRAY(attrs)->ptr[i])->ptr[1];
		if(TYPE(v) == T_ARRAY)
		{		
			r_sz++;
		}
		else
		{
			a_sz++;
		}
	}

	a = ALLOC_N(struct hyperclient_attribute, a_sz);
	range = ALLOC_N(struct hyperclient_range_query, r_sz);

	for(i = 0, ai = 0, ri = 0; i < sz; i++)
	{
		k = RARRAY(RARRAY(attrs)->ptr[i])->ptr[0];
                v = RARRAY(RARRAY(attrs)->ptr[i])->ptr[1];
                if(TYPE(v) == T_ARRAY)
                {
        		range[ri].attr = STR2CSTR(k);
			v1 = RARRAY(v)->ptr[0];
			v2 = RARRAY(v)->ptr[1];
			if(FIXNUM_P(v1) && FIXNUM_P(v2) )
	 		{
				num = (int64_t)NUM2LL(v1);
                                num = htole64(num);
                                memmove((void *)&(range[ri].lower), &num, (size_t)8);

				num = (int64_t)NUM2LL(v2);
                                num = htole64(num);
                                memmove((void *)&(range[ri].upper), &num, (size_t)8);			
			}
			else
			{
				destroy_attrs(&a, a_sz);
		                hyperclient_destroy_attrs(a, a_sz);
        		        free(range);
	                	free(r);
				raise(*(r->ret));
				return Qnil;
			}
	               	ri++;
                }
                else
                {
                	a[ai].attr = STR2CSTR(k);
	                if(FIXNUM_P(v))
        	        {
                	        num = (int64_t)NUM2LL(v);
	                        num = htole64(num);
				a[ai].value = ALLOC_N(char, 8);
                	        memmove((void *)a[i].value, &num, (size_t)8);
                        	a[ai].value_sz = 8;
	                        a[ai].datatype = HYPERDATATYPE_INT64;
        	        }
                	else
	                {	
                        	a[ai].value = STR2CSTR(v);
        	                a[ai].value_sz = strlen(a[ai].value);
                	        a[ai].datatype = HYPERDATATYPE_STRING;
                	}
			ai++;
                }
	}
        r->val = hyperclient_search(r->client, hc_space, a, a_sz, range, r_sz, r->ret, r->attrs, r->attrs_size);
	if(r->val < 0)
        {
		destroy_attrs(&a, a_sz);
                hyperclient_destroy_attrs(a, a_sz);
                free(range);
		free(r);
                raise(*(r->ret));
		return Qnil;
        }
        r->op = 7;      //Search
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(r->op));
        client = Data_Wrap_Struct(chyperclient, 0, 0, r);
	destroy_attrs(&a, a_sz);
        hyperclient_destroy_attrs(a,a_sz);
        free(range);
        return client;	
}

VALUE hc_async_delete(VALUE self, VALUE space, VALUE key)
{
	struct ruby_hyperclient *r;
        char* hc_space;
        char* hc_key;
        size_t hc_key_size;
        int64_t val;
        VALUE client;
        int i;
        r = copy_client(self);
	hc_space = STR2CSTR(space);
        hc_key = STR2CSTR(key);
        hc_key_size = strlen(hc_key);
        r->val = hyperclient_del(r->client, hc_space, hc_key, hc_key_size, r->ret);
        if(r->val<0)
        {
		free(r);
                raise(*(r->ret));
		return Qnil;
        }
        r->op = 4;      // Delete
	hash_insert(&hash, i64tonum(r->val), Qnil);
	hash_insert(&hash_op, i64tonum(r->val), INT2NUM(r->op));
        client = Data_Wrap_Struct(chyperclient, 0, 0, r);
        return client;
}

VALUE hc_atomicdec(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
        VALUE client;
        client = hc_async_atomicdec(self, space, key, attrs);
        return hc_wait(client);
}

VALUE hc_atomicinc(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
	VALUE client, val;
	struct ruby_hyperclient* r;
	client = hc_async_atomicinc(self, space, key, attrs);
	val = hc_wait(client);
	Data_Get_Struct(client, struct ruby_hyperclient, r);
	free(r);
	return val;
}

VALUE hc_condput(VALUE self, VALUE space, VALUE key, VALUE old_attrs, VALUE new_attrs)
{
	VALUE client, val;
	struct ruby_hyperclient *r;	
	client =  hc_async_condput(self, space, key, old_attrs, new_attrs);
	val = hc_wait(client);
	Data_Get_Struct(client, struct ruby_hyperclient, r);
	free(r);
	return val;
}

VALUE hc_search(VALUE self, VALUE space, VALUE attrs)
{
	VALUE client, val, val_array;
	struct ruby_hyperclient *r;
	client = hc_async_search(self, space, attrs);
	if(client == Qnil)
	{
		return Qnil;
	}
	val_array = rb_ary_new();
	while((val=hc_wait(client)) != Qnil)
	{
		rb_ary_unshift(val_array, val);
	}
	Data_Get_Struct(client, struct ruby_hyperclient, r);
	free(r);
	return val_array;
}

VALUE hc_delete(VALUE self, VALUE space, VALUE key)
{
	VALUE client, val;
	struct ruby_hyperclient *r;
	client = hc_async_delete(self, space, key);
	val = hc_wait(client);
	Data_Get_Struct(client, struct ruby_hyperclient, r);
        free(r);
	return val;
}

VALUE hc_get(VALUE self, VALUE space, VALUE key)
{
	VALUE c, val;
	struct ruby_hyperclient* r;
	c = hc_async_get(self, space, key);
	val = hc_wait(c);
	Data_Get_Struct(c, struct ruby_hyperclient, r);
	free(r);	
	return val;
}

VALUE hc_put(VALUE self, VALUE space, VALUE key, VALUE attrs)
{
	VALUE c, val;
	struct ruby_hyperclient* r;
	c = hc_async_put(self, space, key, attrs);
	val = hc_wait(c);
 	Data_Get_Struct(c, struct ruby_hyperclient, r);
        xfree(r);
        return val;
}


VALUE hc_destroy(VALUE self)
{
	struct ruby_hyperclient* r;
	Data_Get_Struct(self, struct ruby_hyperclient, r);
	if(r->client != NULL)
		hyperclient_destroy(r->client);
	free(r->attrs);
	free(r->attrs_size);
	free(r->ret);
	return Qnil;
}

VALUE hc_new(VALUE class, VALUE coordinator, VALUE port)
{
        VALUE argv[2];
	VALUE tdata;
        struct ruby_hyperclient *ptr;
	ptr = ALLOC(struct ruby_hyperclient);
        ptr->client = hyperclient_create(STR2CSTR(coordinator), NUM2INT(port));
	if(ptr->client == NULL)
	{
		rb_raise(rb_eArgError,"Client Error");
		return Qnil;
	}
	ptr->attrs = ALLOC(struct hyperclient_attribute*);
	ptr->attrs_size = ALLOC(size_t);
	ptr->ret = ALLOC(enum hyperclient_returncode);
        tdata = Data_Wrap_Struct(class, 0, 0, ptr);
        argv[0] = coordinator;
        argv[1] = port;
        rb_obj_call_init(tdata, 2, argv);
        return tdata;
}

VALUE hc_init(VALUE self, VALUE coordinator, VALUE port)
{
	rb_iv_set(self, "@coordnator", coordinator);
	rb_iv_set(self, "@port", port);
	return self;
}

void Init_hyperclient() {
  chyperclient = rb_define_class("HyperClient", rb_cObject);
  rb_define_singleton_method(chyperclient, "new", hc_new, 2);
  rb_define_method(chyperclient, "initialize", hc_init, 2);
  rb_define_method(chyperclient, "get", hc_get, 2);
  rb_define_method(chyperclient, "put", hc_put, 3);
  rb_define_method(chyperclient, "async_put", hc_async_put, 3);
  rb_define_method(chyperclient, "async_get", hc_async_get, 2);
  rb_define_method(chyperclient, "destroy", hc_destroy, 0);
  rb_define_method(chyperclient, "condput", hc_condput, 4);
  rb_define_method(chyperclient, "async_condput", hc_async_condput, 4);
  rb_define_method(chyperclient, "atomicinc", hc_atomicinc, 3);
  rb_define_method(chyperclient, "async_atomicinc", hc_async_atomicinc, 3);
  rb_define_method(chyperclient, "atomicdec", hc_atomicdec, 3);
  rb_define_method(chyperclient, "async_atomicdec", hc_async_atomicdec, 3);
  rb_define_method(chyperclient, "search", hc_search, 2);
  rb_define_method(chyperclient, "async_search", hc_async_search, 2);
  rb_define_method(chyperclient, "next", hc_next, 0);
  rb_define_method(chyperclient, "delete", hc_delete, 2);
  rb_define_method(chyperclient, "async_delete", hc_async_delete, 2);
  rb_define_method(chyperclient, "wait", hc_wait, 0);

}


