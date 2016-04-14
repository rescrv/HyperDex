// This is the main DLL file.

#include <hyperclientclr.h>
#include <client/hyperclient.h>
#include <stdlib.h>
#include <string.h>
#include <msclr/marshal.h>
#include <cliext/utility>
#include <marshal.h>
#include <e/endian.h>

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

Int64 unpack_int64(const char *val)
{
	int64_t out;
	e::unpack64le(val, &out);
	return out;
}

Double unpack_float(const char *val)
{
	double out;
	e::unpack64le(val, (int64_t *)&out);
	return out;
}

String ^unpack_string(const char *str, size_t sz)
{
	char *tmp = (char *)malloc(sz + 1);
	memcpy(tmp, str, sz);
	tmp[sz] = '\0';
	String ^output = marshal_as<String ^>(tmp);
	free(tmp);
	return output;
}

Dictionary<String ^, String ^> ^
unpack_map_string_string(const char *pos, size_t rem)
{
	Dictionary<String ^, String ^> ^dict = gcnew Dictionary<String ^, String ^>();
	while (rem >= 4)
	{
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^key = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^val = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<String ^, Int64> ^
unpack_map_string_int64(const char *pos, size_t rem)
{
	Dictionary<String ^, Int64> ^dict = gcnew Dictionary<String ^, Int64>();
	while (rem >= 4)
	{
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^key = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		Int64 val = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<String ^, Double> ^
unpack_map_string_float(const char *pos, size_t rem)
{
	Dictionary<String ^, Double> ^dict = gcnew Dictionary<String ^, Double>();
	while (rem >= 4)
	{
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^key = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		Double val = unpack_float(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<Int64, Double> ^
unpack_map_int64_float(const char *pos, size_t rem)
{
	Dictionary<Int64, Double> ^dict = gcnew Dictionary<Int64, Double>();
	while (rem >= 8)
	{
		Int64 key = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		Double val = unpack_float(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<Int64, Int64> ^
unpack_map_int64_int64(const char *pos, size_t rem)
{
	Dictionary<Int64, Int64> ^dict = gcnew Dictionary<Int64, Int64>();
	while (rem >= 8)
	{
		Int64 key = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		Int64 val = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}


Dictionary<Int64, String ^> ^
unpack_map_int64_string(const char *pos, size_t rem)
{
	Dictionary<Int64, String ^> ^dict = gcnew Dictionary<Int64, String ^>();
	while (rem >= 8)
	{
		Int64 key = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^val = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<Double, Double> ^
unpack_map_float_float(const char *pos, size_t rem)
{
	Dictionary<Double, Double> ^dict = gcnew Dictionary<Double, Double>();
	while (rem >= 8)
	{
		Double key = unpack_float(pos);
		pos += 8;
		rem -= 8;
		Double val = unpack_float(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}

Dictionary<Double, Int64> ^
unpack_map_float_int64(const char *pos, size_t rem)
{
	Dictionary<Double, Int64> ^dict = gcnew Dictionary<Double, Int64>();
	while (rem >= 8)
	{
		Double key = unpack_float(pos);
		pos += 8;
		rem -= 8;
		Int64 val = unpack_int64(pos);
		pos += 8;
		rem -= 8;
		dict->Add(key, val);
	}
	return dict;
}


Dictionary<Double, String ^> ^
unpack_map_float_string(const char *pos, size_t rem)
{
	Dictionary<Double, String ^> ^dict = gcnew Dictionary<Double, String ^>();
	while (rem >= 8)
	{
		Double key = unpack_float(pos);
		pos += 8;
		rem -= 8;
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		String ^val = unpack_string(pos, sz);
		pos += sz;
		rem -= sz;
		dict->Add(key, val);
	}
	return dict;
}


SortedSet<String ^> ^
unpack_set_string(const char *pos, size_t rem)
{
	SortedSet<String ^> ^lst = gcnew SortedSet<String ^>();
	while (rem >= 4)
	{
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		lst->Add(unpack_string(pos, sz));
		pos += sz;
		rem -= sz;
	}
	return lst;
}

SortedSet<Double> ^
unpack_set_float(const char *pos, size_t rem)
{
	SortedSet<Double> ^lst = gcnew SortedSet<Double>();
	while (rem >= 8)
	{
		lst->Add(unpack_float(pos));
		pos += 8;
		rem -= 8;
	}
	return lst;
}

SortedSet<Int64> ^
unpack_set_int64(const char *pos, size_t rem)
{
	SortedSet<Int64> ^lst = gcnew SortedSet<Int64>();
	while (rem >= 8)
	{
		lst->Add(unpack_int64(pos));
		pos += 8;
		rem -= 8;
	}
	return lst;
}


List<String ^> ^
unpack_list_string(const char *pos, size_t rem)
{
	List<String ^> ^lst = gcnew List<String ^>();
	while (rem >= 4)
	{
		uint32_t sz;
		e::unpack32le(pos, &sz);
		pos += 4;
		rem -= 4;
		lst->Add(unpack_string(pos, sz));
		pos += sz;
		rem -= sz;
	}
	return lst;
}

List<Double> ^
unpack_list_float(const char *pos, size_t rem)
{
	List<Double> ^lst = gcnew List<Double>();
	while (rem >= 8)
	{
		lst->Add(unpack_float(pos));
		pos += 8;
		rem -= 8;
	}
	return lst;
}

List<Int64> ^
unpack_list_int64(const char *pos, size_t rem)
{
	List<Int64> ^lst = gcnew List<Int64>();
	while (rem >= 8)
	{
		lst->Add(unpack_int64(pos));
		pos += 8;
		rem -= 8;
	}
	return lst;
}


HyperClient::HyperClient(String ^coordinator, uint16_t port)
{
	WSADATA wsa;
	if (WSAStartup(MAKEWORD(2, 2), &wsa))
		abort();
	const char *coord;
	mc = gcnew marshal_context();
	coord = mc->marshal_as<const char *>( coordinator );
	_hc = new hyperclient(coord, port);
}

HyperClient::~HyperClient()
{
	this->!HyperClient();
}

HyperClient::!HyperClient()
{
	delete _hc;
	_hc = nullptr;
}

Dictionary<String ^, Object ^> ^
HyperClient::get(String ^space, String ^key)
{
	Dictionary<String ^, Object ^> ^output = gcnew Dictionary<String ^, Object ^>;
	marshal_context context;
	hyperclient_returncode st;
	hyperclient_attribute *attrs;
	size_t attrs_sz;
	int64_t ret = _hc->get(context.marshal_as<const char *>(space),
	                       context.marshal_as<const char *>(key),
	                       key->Length, &st, &attrs, &attrs_sz);
	if (ret < 0)
	{
		THROW_EXCEPTION(st);
	}
	while (1)
	{
		hyperclient_returncode rc;
		int64_t lret = _hc->loop(-1, &rc);
		if (lret == ret)
		{
			if (rc == HYPERCLIENT_SUCCESS)
			{
				for (int i = 0; i < attrs_sz; ++i)
				{
					if (attrs[i].value_sz == 0)
					{
						continue;
					}
					switch (attrs[i].datatype)
					{
					case HYPERDATATYPE_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_INT64:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_int64(attrs[i].value));
						break;
					case HYPERDATATYPE_FLOAT:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_float(attrs[i].value));
						break;
					case HYPERDATATYPE_SET_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_SET_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_SET_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_KEYONLY:
					default:
						THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
						break;
					}
				}
			}
			else
			{
				THROW_EXCEPTION(rc);
			}
			break;
		}
	}
	return output;
}

#define WAIT_FOR_STATUS \
	while(1) \
	{ \
		hyperclient_returncode rc; \
		int64_t lret = _hc->loop(-1, &rc); \
		if(lret == ret) \
		{ \
			if(rc == HYPERCLIENT_SUCCESS) \
			{ \
				return true; \
			} \
			\
			THROW_EXCEPTION(rc); \
		} \
	} \
	\
	return false; \

#define HYPERCLIENT_CLRDEF(OPNAME) \
	bool \
	HyperClient:: ## OPNAME(String ^space, String ^key, Dictionary<String ^, Object ^> ^ attrs) \
	{ \
		size_t size; \
		for each(KeyValuePair<String ^, Object ^> kvp in attrs) \
		{ \
			if (kvp.Value->GetType()->IsGenericType \
			    && kvp.Value->GetType()->GetGenericTypeDefinition() == List::typeid) \
			{ \
				if(kvp.Value->GetType() == String::typeid) \
					size += 1 + ((List<String ^>^)kvp.Value)->Count; \
				if(kvp.Value->GetType() == Int64::typeid) \
					size += 1 + ((List<Int64>^)kvp.Value)->Count; \
				if(kvp.Value->GetType() == Double::typeid) \
					size += 1 + ((List<Double>^)kvp.Value)->Count; \
			} \
			else if (kvp.Value->GetType()->IsGenericType \
			         && kvp.Value->GetType()->GetGenericTypeDefinition() == SortedSet::typeid) \
			{ \
				if(kvp.Value->GetType() == String::typeid) \
					size += 1 + ((SortedSet<String ^>^)kvp.Value)->Count; \
				if(kvp.Value->GetType() == Int64::typeid) \
					size += 1 + ((SortedSet<Int64>^)kvp.Value)->Count; \
				if(kvp.Value->GetType() == Double::typeid) \
					size += 1 + ((SortedSet<Double>^)kvp.Value)->Count; \
			} \
			else \
			{ \
				size ++; \
			} \
		} \
		marshal_context context; \
		hyperclient_returncode st; \
		int64_t ret = _hc->OPNAME(context.marshal_as<const char*>(space), \
		                          context.marshal_as<const char*>(key), \
		                          key->Length, \
		                          context.marshal_as<hyperclient_attribute*>(attrs), \
		                          size, &st); \
		if(ret < 0) \
		{ \
			THROW_EXCEPTION(st); \
		} \
		WAIT_FOR_STATUS \
	}

	HYPERCLIENT_CLRDEF(put)
	HYPERCLIENT_CLRDEF(put_if_not_exist)
	HYPERCLIENT_CLRDEF(atomic_add)
	HYPERCLIENT_CLRDEF(atomic_sub)
	HYPERCLIENT_CLRDEF(atomic_mul)
	HYPERCLIENT_CLRDEF(atomic_div)
	HYPERCLIENT_CLRDEF(atomic_mod)
	HYPERCLIENT_CLRDEF(atomic_and)
	HYPERCLIENT_CLRDEF(atomic_or)
	HYPERCLIENT_CLRDEF(atomic_xor)
	HYPERCLIENT_CLRDEF(string_prepend)
	HYPERCLIENT_CLRDEF(string_append)
	HYPERCLIENT_CLRDEF(list_lpush)
	HYPERCLIENT_CLRDEF(list_rpush)
	HYPERCLIENT_CLRDEF(set_add)
	HYPERCLIENT_CLRDEF(set_remove)
	HYPERCLIENT_CLRDEF(set_intersect)
	HYPERCLIENT_CLRDEF(set_union)

#define HYPERCLIENT_MAP_CLRDEF(OPNAME) \
	bool \
	HyperClient:: map_ ## OPNAME(String ^space, String ^key, Dictionary<String ^, Dictionary<Object^, Object ^>^ > ^ attrs) \
	{ \
		marshal_context context; \
		hyperclient_returncode st; \
		size_t size = 0; \
		for each(KeyValuePair<String ^, Dictionary<Object ^, Object ^>^ > attrkvp in attrs) \
		{ \
			size += attrkvp.Value->Count; \
		} \
		int64_t ret = _hc->map_ ## OPNAME(context.marshal_as<const char*>(space), \
		                                  context.marshal_as<const char*>(key), \
		                                  key->Length, \
		                                  context.marshal_as<hyperclient_map_attribute*>(attrs), \
		                                  size, &st); \
		if(ret < 0) \
		{ \
			THROW_EXCEPTION(st); \
		} \
		WAIT_FOR_STATUS \
	}

	HYPERCLIENT_MAP_CLRDEF(add)
	HYPERCLIENT_MAP_CLRDEF(remove)
	HYPERCLIENT_MAP_CLRDEF(atomic_add)
	HYPERCLIENT_MAP_CLRDEF(atomic_sub)
	HYPERCLIENT_MAP_CLRDEF(atomic_mul)
	HYPERCLIENT_MAP_CLRDEF(atomic_div)
	HYPERCLIENT_MAP_CLRDEF(atomic_mod)
	HYPERCLIENT_MAP_CLRDEF(atomic_and)
	HYPERCLIENT_MAP_CLRDEF(atomic_or)
	HYPERCLIENT_MAP_CLRDEF(atomic_xor)
	HYPERCLIENT_MAP_CLRDEF(string_prepend)
	HYPERCLIENT_MAP_CLRDEF(string_append)

	List<Dictionary<String ^, Object ^>^ > ^
	HyperClient::search(String ^space, Dictionary<String ^, Object ^> ^checks)
	{
		List<Dictionary<String ^, Object ^>^ > ^toValue = gcnew List<Dictionary<String ^, Object ^>^ >();
		marshal_context context;
		hyperclient_returncode st;
		struct hyperclient_attribute *attrs;
		size_t attrs_sz;
		size_t checks_sz;
		for each(KeyValuePair<String ^, Object ^> kvp in checks)
		{
			if (kvp.Value->GetType()->IsGenericType
			    && kvp.Value->GetType()->GetGenericTypeDefinition() == KeyValuePair::typeid)
			{
				++checks_sz;
			}
			++checks_sz;
		}
		int64_t ret = _hc->search(context.marshal_as<const char *>(space),
		                          context.marshal_as<struct hyperclient_attribute_check *>(checks),
		                          checks_sz, &st, &attrs, &attrs_sz);
		if (ret < 0)
		{
			THROW_EXCEPTION(st);
		}
		while (1)
		{
			hyperclient_returncode rc;
			int64_t lret = _hc->loop(-1, &rc);
			if (lret < 0)
			{
				THROW_EXCEPTION(rc);
			}
			else if (lret == ret)
			{
				Dictionary<String ^, Object ^> ^output = gcnew Dictionary<String ^, Object ^>();
				for (int i = 0; i < attrs_sz; ++i)
				{
					switch (attrs[i].datatype)
					{
					case HYPERDATATYPE_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_INT64:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_int64(attrs[i].value));
						break;
					case HYPERDATATYPE_FLOAT:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_float(attrs[i].value));
						break;
					case HYPERDATATYPE_SET_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_SET_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_SET_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_set_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_LIST_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_list_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_string_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_INT64_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_int64_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_STRING:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_string(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_INT64:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_int64(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_FLOAT_FLOAT:
					{
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            unpack_map_float_float(attrs[i].value, attrs[i].value_sz));
						break;
					}
					case HYPERDATATYPE_MAP_STRING_KEYONLY:
					default:
						THROW_EXCEPTION(HYPERCLIENT_WRONGTYPE);
						break;
					}
				}
				toValue->Add(output);
				if (rc == HYPERCLIENT_SEARCHDONE)
				{
					break;
				}
			}
		}
		return toValue;
	}

	List<Dictionary<String ^, Object ^>^ > ^
	HyperClient::sorted_search(String ^space, String ^sortby,
	                           UInt64 limit, bool maximize, Dictionary<String ^, Object ^> ^checks)
	{
		List<Dictionary<String ^, Object ^>^ > ^toValue = gcnew List<Dictionary<String ^, Object ^>^ >();
		marshal_context context;
		hyperclient_returncode st;
		struct hyperclient_attribute *attrs;
		size_t attrs_sz;
		size_t checks_sz;
		for each(KeyValuePair<String ^, Object ^> kvp in checks)
		{
			if (kvp.Value->GetType()->IsGenericType
			    && kvp.Value->GetType()->GetGenericTypeDefinition() == KeyValuePair::typeid)
			{
				++checks_sz;
			}
			++checks_sz;
		}
		int64_t ret = _hc->sorted_search(context.marshal_as<const char *>(space),
		                                 context.marshal_as<struct hyperclient_attribute_check *>(checks),
		                                 checks_sz, context.marshal_as<const char *>(sortby), limit, maximize,
		                                 &st, &attrs, &attrs_sz);
		if (ret < 0)
		{
			THROW_EXCEPTION(st);
		}
		while (1)
		{
			hyperclient_returncode rc;
			int64_t lret = _hc->loop(-1, &rc);
			if (lret < 0)
			{
				THROW_EXCEPTION(rc);
			}
			else if (lret == ret)
			{
				Dictionary<String ^, Object ^> ^output = gcnew Dictionary<String ^, Object ^>();
				for (int i = 0; i < attrs_sz; ++i)
				{
					switch (attrs[i].datatype)
					{
					case HYPERDATATYPE_STRING:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            marshal_as<String ^>(attrs[i].value));
						break;
					case HYPERDATATYPE_INT64:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            *(int64_t *)attrs[i].value);
						break;
					case HYPERDATATYPE_FLOAT:
						output->Add(marshal_as<String ^>(attrs[i].attr),
						            *(double *)attrs[i].value);
						break;
					default:
						break;
					}
				}
				toValue->Add(output);
				if (rc == HYPERCLIENT_SEARCHDONE)
				{
					break;
				}
			}
		}
		return toValue;
	}


	UInt64
	HyperClient::count(String ^space, Dictionary<String ^, Object ^> ^checks)
	{
		marshal_context context;
		hyperclient_returncode st;
		size_t checks_sz;
		uint64_t result;
		for each(KeyValuePair<String ^, Object ^> kvp in checks)
		{
			if (kvp.Value->GetType()->IsGenericType
			    && kvp.Value->GetType()->GetGenericTypeDefinition() == KeyValuePair::typeid)
			{
				++checks_sz;
			}
			++checks_sz;
		}
		int64_t ret = _hc->count(context.marshal_as<const char *>(space),
		                         context.marshal_as<struct hyperclient_attribute_check *>(checks),
		                         checks_sz, &st, &result);
		if (ret < 0)
		{
			THROW_EXCEPTION(st);
		}
		while (1)
		{
			hyperclient_returncode rc;
			int64_t lret = _hc->loop(-1, &rc);
			if (lret < 0)
			{
				THROW_EXCEPTION(rc);
			}
			else if (lret == ret)
			{
				if (rc == HYPERCLIENT_SUCCESS)
				{
					return result;
				}
				THROW_EXCEPTION(rc);
			}
		}
	}


	bool
	HyperClient::del(String ^space, String ^key)
	{
		marshal_context context;
		hyperclient_returncode st;
		int64_t ret = _hc->del(context.marshal_as<const char *>(space),
		                       context.marshal_as<const char *>(key),
		                       key->Length, &st);
		if (ret < 0)
		{
			THROW_EXCEPTION(st);
		}
		WAIT_FOR_STATUS
	}
	}
