/* C Includes */
#include <stdio.h>
#include <stdlib.h>

/* HyperDex Includes */
#include <hyperdex/datastructures.h>

int
main(int argc, const char* argv[])
{
   int64_t num;
   const char* value = "\x01\x00\x00\x00\x00\x00\x00\x00"
                       "\xff\xff\xff\xff\xff\xff\xff\xff"
                       "\xef\xbe\xad\xde\x00\x00\x00\x00";
   size_t value_sz = 24;
   int status = 0;
   hyperdex_ds_iterator iter;
   hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_INT64, value, value_sz);

   while ((status = hyperdex_ds_iterate_list_int_next(&iter, &num)) > 0)
   {
       printf("%ld\n", num);
   }

   if (status < 0)
   {
       printf("error: corrupt list\n");
   }

   return EXIT_SUCCESS;
}
