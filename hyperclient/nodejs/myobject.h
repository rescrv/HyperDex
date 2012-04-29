#define BUILDING_NODE_EXTENSION
#ifndef MYOBJECT_H
#define MYOBJECT_H
#include <inttypes.h>
#include <node.h>
#include "hyperclient/hyperclient.h"


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
		static v8::Handle<v8::Value> async_atomicinc(const v8::Arguments& args);
                static v8::Handle<v8::Value> atomicinc(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_atomicdec(const v8::Arguments& args);
                static v8::Handle<v8::Value> atomicdec(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_search(const v8::Arguments& args);
                static v8::Handle<v8::Value> search(const v8::Arguments& args);
		static v8::Handle<v8::Value> async_del(const v8::Arguments& args);
                static v8::Handle<v8::Value> del(const v8::Arguments& args);
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

#endif
