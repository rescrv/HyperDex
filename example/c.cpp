#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <assert.h>
#include <hyperdex/admin.hpp>
#include <hyperdex/client.hpp>
#include <hyperdex/client.h>

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
	attr.attr = "first";
	if (argc > 1)
	{
		char *pointer = NULL;
		int arg1 = atoi(argv[1]);
		if (arg1 == 1)
		{
			attr.value = "Hello, World!";
			attr.value_sz = strlen(attr.value);
		}
		else
		{
			size_t p_len = 100 * 1024 * 1024;
			char *p = (char *)malloc(p_len);
			const char *file = "./data.txt";
			fstream fs;
			fs.open(file, ios::in | ios::binary);
			if (!fs)
			{
				cerr << "not exist file: " << file << endl;
				exit(EXIT_FAILURE);
			}
			fs.getline(p, p_len);
			fs.close();
			attr.value = p;
			attr.value_sz = p_len;
			pointer = p;
		}
		int arg2 = argv[2] == NULL ? 0 : atoi(argv[2]);
		attr.datatype = HYPERDATATYPE_STRING;
		if (arg2 == 0)
		{
			if (arg1 == 1)
				printf("put \"Hello World!\"\n");
			else
				printf("put \"Large Value!\"\n");
			op_id = hyperdex_client_put(client, ns, "some key", 8, &attr, 1, &op_status);
			if (op_id < 0)
			{
				cout << hyperdex_client_error_message(client) << endl;
				return EXIT_FAILURE;
			}
			loop_id = hyperdex_client_loop(client, -1, &loop_status);
			if (loop_id < 0)
			{
				cout << hyperdex_client_error_message(client) << endl;
				return EXIT_FAILURE;
			}
			assert(op_id == loop_id);
		}
		op_id = hyperdex_client_get(client, ns, "some key", 8, &op_status, &attrs, &attrs_sz);
		if (op_id < 0)
		{
			cout << hyperdex_client_error_message(client) << endl;
			return EXIT_FAILURE;
		}
		loop_id = hyperdex_client_loop(client, -1, &loop_status);
		if (loop_id < 0)
		{
			cout << hyperdex_client_error_message(client) << endl;
			return EXIT_FAILURE;
		}
		assert(op_id == loop_id);
		printf("get done\n");
		for (i = 0; i < attrs_sz; ++i)
		{
			printf("got attribute \"%s\" = \"%.*s\"\n",
			       attrs[i].attr, attrs[i].value_sz, attrs[i].value);
		}
		if (pointer)
			free(pointer);
		hyperdex_client_destroy_attrs(attrs, attrs_sz);
	}
	else
	{
		check.attr = "first";
		check.value = "John";
		check.value_sz = strlen(check.value);
		check.datatype = HYPERDATATYPE_STRING;
		check.predicate = HYPERPREDICATE_EQUALS;
		count = 0;
		check_len += strlen(check.attr);
		check_len += check.value_sz;
		check_len += sizeof(size_t);
		check_len += sizeof(hyperdatatype);
		check_len += sizeof(hyperpredicate);
		check_len = 1;
		op_id = hyperdex_client_count(client, ns, &check, check_len, &op_status, &count);
		if (op_id < 0)
		{
			cout << "count failed: " << hyperdex_client_error_message(client) << endl;
			return EXIT_FAILURE;
		}
		loop_id = hyperdex_client_loop(client, -1, &loop_status);
		if (loop_id < 0)
		{
			cout << hyperdex_client_error_message(client) << endl;
			return EXIT_FAILURE;
		}
		assert(op_id == loop_id);
		printf("count: %ld\n", count);
	}
	hyperdex_client_destroy(client);
	return EXIT_SUCCESS;
}
