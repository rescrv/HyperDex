#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <hyperdex/admin.hpp>
#include <hyperdex/client.hpp>
#include <hyperdex/client.h>
#include <e/endian.h>

using namespace std;

int
main(int argc, const char *argv[])
{
	struct hyperdex_client *client = NULL;
	struct hyperdex_client_attribute attr;
	struct hyperdex_client_attribute_check check;
	const struct hyperdex_client_attribute *attrs = NULL;
	size_t attrs_sz = 0;
	size_t check_len = 0;
	uint64_t count;
	int64_t op_id;
	enum hyperdex_client_returncode op_status;
	int64_t loop_id;
	enum hyperdex_client_returncode loop_status;
	size_t i;
	const char *ns = "phonebook";
	client = hyperdex_client_create("127.0.0.1", 1982);
	const char *key = "jsmith3";
	size_t key_len = strlen(key);
	//check.attr = "first";
	//check.value = "Jack";
	//check.value_sz = strlen(check.value);
	//check.datatype = HYPERDATATYPE_STRING;
	//check.predicate = HYPERPREDICATE_EQUALS;
	op_id = hyperdex_client_get(client, ns, key, key_len, &op_status, &attrs, &attrs_sz);
	if (op_id < 0)
	{
		cerr << "get failed: " << hyperdex_client_error_message(client) << endl;
		hyperdex_client_destroy(client);
		return EXIT_FAILURE;
	}
	while (true)
	{
		loop_id = hyperdex_client_loop(client, -1, &loop_status);
		if (loop_id < 0)
		{
			cerr << hyperdex_client_error_message(client) << endl;
			break;
		}
		if (op_status == HYPERDEX_CLIENT_SEARCHDONE)
		{
			break;
		}
		assert(op_id == loop_id);
		if (loop_status == HYPERDEX_CLIENT_SUCCESS)
		{
			printf("get done(%d)\n", attrs_sz);
			for (i = 0; i < attrs_sz; ++i)
			{
				switch (attrs[i].datatype)
				{
				case HYPERDATATYPE_STRING:
					printf("got attribute \"%s\" = \"%.*s\"\n", attrs[i].attr, attrs[i].value_sz, attrs[i].value);
					break;
				case HYPERDATATYPE_INT64:
				{
					uint64_t num = 0;
					e::unpack64le(attrs[i].value, &num);
					printf("got attribute \"%s\" = \"%lu\"\n", attrs[i].attr, num);
				}
				break;
				case HYPERDATATYPE_FLOAT:
				{
					double num = 0;
					e::unpackdoublele(attrs[i].value, &num);
					printf("got attribute \"%s\" = \"%f\"\n", attrs[i].attr, num);
				}
				break;
				default:
					printf("got error attribute\n");
				}
			}
		}
	}
	hyperdex_client_destroy_attrs(attrs, attrs_sz);
	hyperdex_client_destroy(client);
	return EXIT_SUCCESS;
}
