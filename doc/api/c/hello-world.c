/* WARNING:  This code omits all error checking and is for illustration only */
/* HyperDex Includes */
#include <hyperdex/client.h>
int
main(int argc, const char* argv[])
{
    struct hyperdex_client* client = NULL;
    struct hyperdex_client_attribute attr;
    struct hyperdex_client_attribute* attrs;
    size_t attrs_sz = 0;
    int64_t op_id;
    enum hyperdex_client_returncode op_status;
    int64_t loop_id;
    enum hyperdex_client_returncode loop_status;
    size_t i;

    client = hyperdex_client_create("127.0.0.1", 1982);

    attr.attr = "v";
    attr.value = "Hello World!";
    attr.value_sz = strlen(attr.value);
    attr.datatype = HYPERDATATYPE_STRING;

    /* perform the "put" */
    op_id = hyperdex_client_put(client, "kv", "some key", 8,
                                &attr, 1, &op_status);
    loop_id = hyperdex_client_loop(client, -1, &loop_status);
    printf("put \"Hello World!\"\n");

    /* perform the "get" */
    op_id = hyperdex_client_get(client, "kv", "some key", 8,
                                &op_status, &attrs, &attrs_sz);
    loop_id = hyperdex_client_loop(client, -1, &loop_status);
    printf("get done\n");

    for (i = 0; i < attrs_sz; ++i)
    {
        printf("got attribute \"%s\" = \"%.*s\"\n",
               attrs[i].attr, attrs[i].value_sz, attrs[i].value);
    }

    hyperdex_client_destroy_attrs(attrs, attrs_sz);
    hyperdex_client_destroy(client);
    return EXIT_SUCCESS;
}
