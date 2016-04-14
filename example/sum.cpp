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
#include "hyperdex/admin.hpp"
#include "hyperdex/client.hpp"
#include "hyperdex/client.h"
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
	uint64_t sum = 0;
	int64_t op_id;
	enum hyperdex_client_returncode op_status;
	int64_t loop_id;
	enum hyperdex_client_returncode loop_status;
	size_t i;
	const char *ns = "phonebook";
	client = hyperdex_client_create("127.0.0.1", 1982);
	const char *row_key = "phone";
	check.attr = "first";
	check.value = "Jack";
	check.value_sz = strlen(check.value);
	check.datatype = HYPERDATATYPE_STRING;
	check.predicate = HYPERPREDICATE_EQUALS;
	op_id = hyperdex_client_sum(client, ns, &check, 1, row_key, &op_status, &sum);
	if (op_id < 0)
	{
		cerr << "search failed: " << hyperdex_client_error_message(client) << endl;
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
		//if (op_status == HYPERDEX_CLIENT_SEARCHDONE) {
		//	break;
		//}
		assert(op_id == loop_id);
		if (loop_status == HYPERDEX_CLIENT_SUCCESS)
		{
			printf("get done(%llu)\n", sum);
		}
	}
	hyperdex_client_destroy_attrs(attrs, attrs_sz);
	hyperdex_client_destroy(client);
	return EXIT_SUCCESS;
}
