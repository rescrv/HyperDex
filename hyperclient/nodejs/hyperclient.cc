#define BUILDING_NODE_EXTENSION
#include <node.h>
#include "hyperclient/nodejs/myobject.h"

using namespace v8;

Handle<Value> CreateObject(const Arguments& args) {
  HandleScope scope;
  return scope.Close(HyperClient::NewInstance(args));
}

void InitAll(Handle<Object> target) {
  	HyperClient::Init();

  	target->Set(String::NewSymbol("Client"), FunctionTemplate::New(CreateObject)->GetFunction());

}

NODE_MODULE(hyperclient, InitAll)
