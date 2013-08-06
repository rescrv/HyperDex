Data Structures
---------------

HyperDex encodes as a bytestring all data structures passed between the
application and HyperDex.  This format of this bytestring varies according to
its type.  Below we describe the format of HyperDex data structures, and an API
for serializing and deserializing HyperDex's datatypes.

Bytestring Format
~~~~~~~~~~~~~~~~~

The format of the data structures is defined to be the same on all platforms.

For each format, Python-like psuedocode is provided that shows example
encodings.

string
++++++

A string is an 8-bit byte string.  HyperDex is agnostic to the contents of the
string, and it may contain any bytes, including ``\x00``.

Example:

.. sourcecode:: python

   >>> encode_string('Hello\x00World!')
   b'Hello\x00World!'

By convention, the trailing ``NULL`` should be omitted for C-strings to ensure
interoperability across languages.

int
+++

Integers are encoded as signed 8-byte little-endian integers.

Examples:

.. sourcecode:: python

   >>> encode_int(1)
   b'\x01\x00\x00\x00\x00\x00\x00\x00'
   >>> encode_int(-1)
   b'\xff\xff\xff\xff\xff\xff\xff\xff'
   >>> encode_int(0xdeadbeef)
   b'\xef\xbe\xad\xde\x00\x00\x00\x00'

float
+++++

Floats are encoded as IEEE 754 binary64 values in little-endian format.

Examples:

.. sourcecode:: python

   >>> encode_double(0)
   b'\x00\x00\x00\x00\x00\x00\x00\x00'
   >>> encode_double(3.1415)
   b'o\x12\x83\xc0\xca!\t@'


list(string)
++++++++++++

Lists of strings are encoded by concatenating the encoding of each string,
prefixed by an unsigned 4-byte little endian integer indicating the length of
the string.

Examples:

.. sourcecode:: python

   >>> encode_list_string([])
   b''
   >>> encode_list_string(['hello', 'world'])
   b'\x05\x00\x00\x00hello\x05\x00\x00\x00world'

list(int)
+++++++++

Lists of integers are encoded by concatenating the encoded form of each integer.

Examples:

.. sourcecode:: python

   >>> encode_list_int([])
   b''
   >>> encode_list_int([1, -1, 0xdeadbeef])
   b'\x01\x00\x00\x00\x00\x00\x00\x00' \
   b'\xff\xff\xff\xff\xff\xff\xff\xff' \
   b'\xef\xbe\xad\xde\x00\x00\x00\x00'

list(floats)
++++++++++++

Lists of floats are encoded by concatenating the encoded form of each float.

Examples:

.. sourcecode:: python

   >>> encode_list_float([])
   b''
   >>> encode_list_float([0, 3.1415])
   b'\x00\x00\x00\x00\x00\x00\x00\x00' \
   b'o\x12\x83\xc0\xca!\t@'

set(string)
+++++++++++

Sets of strings are encoded by concatenating the encoding of each string in
sorted order, where each string is prefixed by an unsigned 4-byte little endian
integer indicating the length of the string.

Examples:

.. sourcecode:: python

   >>> encode_set_string([])
   b''
   >>> encode_set_string(['world', 'hello'])
   b'\x05\x00\x00\x00hello\x05\x00\x00\x00world'

set(int)
++++++++

Sets of integers are encoded by concatenating the encoded form of each integer
in sorted order.

Examples:

.. sourcecode:: python

   >>> encode_set_int([])
   b''
   >>> encode_set_int([1, -1, 0xdeadbeef])
   b'\xff\xff\xff\xff\xff\xff\xff\xff' \
   b'\x01\x00\x00\x00\x00\x00\x00\x00' \
   b'\xef\xbe\xad\xde\x00\x00\x00\x00'

set(float)
++++++++++

Sets of floats are encoded by concatenating the encoded form of each float in
sorted order.

Examples:

.. sourcecode:: python

   >>> encode_set_float([])
   b''
   >>> encode_set_float([3.1415, 0])
   b'\x00\x00\x00\x00\x00\x00\x00\x00' \
   b'o\x12\x83\xc0\xca!\t@'

map(string, string)
+++++++++++++++++++

Maps from strings to strings are formed by encoding the individual elements,
each prefixed by an unsigned 4-byte little endian integer indicating their
length.  The pairs of elements are stored in sorted order according to the first
element of the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_string_string({})
   b''
   >>> encode_map_string_string({'hello': 'world',
   ...                           'map key': 'map val',
   ...                           'map', 'encoding'})
   b'\x05\x00\x00\x00hello\x05\x00\x00\x00world' \
   b'\x03\x00\x00\x00map\x08\x00\x00\x00encoding' \
   b'\x07\x00\x00\x00map key\x07\x00\x00\x00map val'

map(string, int)
++++++++++++++++

Maps from strings to ints are formed by encoding the individual elements, where
keys are prefixed by an unsigned 4-byte little endian integer indicating their
length.  The pairs of elements are stored in sorted order according to the first
element of the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_string_int({})
   b''
   >>> encode_map_string_int({'world': -1,
   ...                        'hello': 1})
   b'\x05\x00\x00\x00hello\x01\x00\x00\x00\x00\x00\x00\x00' \
   b'\x05\x00\x00\x00world\xff\xff\xff\xff\xff\xff\xff\xff'

map(string, float)
++++++++++++++++++

Maps from strings to ints are formed by encoding the individual elements, where
keys are prefixed by an unsigned 4-byte little endian integer indicating their
length.  The pairs of elements are stored in sorted order according to the first
element of the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_string_float({})
   b''
   >>> encode_map_string_float({'zero': 0,
   ...                          'pi': 3.1415})
   b'\x02\x00\x00\x00pio\x12\x83\xc0\xca!\t@' \
   b'\x04\x00\x00\x00zero\x00\x00\x00\x00\x00\x00\x00\x00'

map(int, string)
++++++++++++++++

Maps from ints to strings are formed by encoding the individual elements, where
values are prefixed by an unsigned 4-byte little endian integer indicating their
length.  The pairs of elements are stored in sorted order according to the first
element of the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_int_string({})
   b''
   >>> encode_map_int_string({1: 'hello',
                              -1: 'world'})
   b'\xff\xff\xff\xff\xff\xff\xff\xff\x05\x00\x00\x00world' \
   b'\x01\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00hello'

map(int, int)
+++++++++++++

Maps from ints to ints are formed by encoding the individual elements.  The
pairs of elements are stored in sorted order according to the first element of
the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_int_int({})
   b''
   >>> encode_map_int_int({1: 0xdeadbeef,
   ...                     -1: 0x1eaff00d})
   b'\xff\xff\xff\xff\xff\xff\xff\xff\x0d\xf0\xaf\x1e\x00\x00\x00\x00' \
   b'\x01\x00\x00\x00\x00\x00\x00\x00\xef\xbe\xad\xde\x00\x00\x00\x00'

map(int, float)
+++++++++++++++

Maps from ints to floats are formed by encoding the individual elements.  The
pairs of elements are stored in sorted order according to the first element of
the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_int_float({})
   b''
   >>> encode_map_int_float({1: 0,
   ...                       -1: 3.1415})
   b'\xff\xff\xff\xff\xff\xff\xff\xffo\x12\x83\xc0\xca!\t@' \
   b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

map(float, string)
++++++++++++++++++

Maps from floats to strings are formed by encoding the individual elements, where
values are prefixed by an unsigned 4-byte little endian integer indicating their
length.  The pairs of elements are stored in sorted order according to the first
element of the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_float_string({})
   b''
   >>> encode_map_float_string({0: 'hello',
   ...                          3.1415: 'world'})
   b'\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00hello' \
   b'o\x12\x83\xc0\xca!\t@\x05\x00\x00\x00world'

map(float, int)
+++++++++++++++

Maps from floats to ints are formed by encoding the individual elements.  The
pairs of elements are stored in sorted order according to the first element of
the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_float_int({})
   b''
   >>> encode_map_float_int({0: 1,
   ...                       3.1415: -1})
   b'\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00' \
   b'o\x12\x83\xc0\xca!\t@\xff\xff\xff\xff\xff\xff\xff\xff'

map(float, float)
+++++++++++++++++

Maps from floats to floats are formed by encoding the individual elements.  The
pairs of elements are stored in sorted order according to the first element of
the pair (the map's key).

Examples:

.. sourcecode:: python

   >>> encode_map_float_float({})
   b''
   >>> encode_map_float_float({0: 1,
   ...                         3.1415: -1})
   b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?' \
   b'o\x12\x83\xc0\xca!\t@\x00\x00\x00\x00\x00\x00\xf0\xbf'

``struct hyperdex_ds_arena``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The packing routines described below may occasionally have to allocate memory
into which the encoded forms of the datatypes are copied.  To free the
programmer from the burden of having to manually free each of these pieces of
memory, the data structures API allocates all memory via an instance of
``struct hyperdex_ds_arena``.  Via a single call to
``hyperdex_ds_arena_destroy``, all memory allocated via the arena is freed.

``struct hyperdex_ds_arena`` is an incomplete type and its internals are subject
to change.  To create an arena, call ``hyperdex_ds_arena_create``.  The arena
should subsequently be destroyed via ``hyperdex_ds_arena_destroy``.

.. function:: hyperdex_ds_arena_create()

   Create a new arena for allocating memory

   :return:  A newly allocated arena instance
   :rtype: ``struct hyperdex_ds_arena*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_ds_arena*
      hyperdex_ds_arena_create();

   On success, this function returns a non-null pointer for the new arena.  On
   failure, the function returns ``NULL``, indicating that allocating memory
   failed.

.. function:: hyperdex_ds_arena_destroy(arena)

   :param arena: An arena allocated via ``hyperdex_ds_arena_create``

   The full prototype is:

   .. sourcecode:: c

      void
      hyperdex_ds_arena_destroy(struct hyperdex_ds_arena* arena);

   This function always succeeds.

Allocating Client-library Structs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When interacting with the client library, especially when writing bindings for a
new language, it is necessary to dynamically allocate arrays of the client
library's structures.  The following functions enable you to allocate these
structures using an arena, thus obviating the need to free them individually.

.. function:: hyperdex_ds_allocate_attribute(arena, sz)

   Allocate an array of ``struct hyperdex_client_attribute``.

   :param arena: An arena allocated via ``hyperdex_ds_arena_create``
   :type arena: ``struct hyperdex_ds_arena``
   :param sz:  The number of elements in the array
   :type sz: ``size_t``
   :return: A newly allocated array
   :rtype: ``struct hyperdex_client_attribute*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_client_attribute*
      hyperdex_ds_allocate_attribute(struct hyperdex_ds_arena* arena, size_t sz);

   On success, this function returns a non-null pointer to containing ``sz``
   elements. On failure, the function returns ``NULL``, indicating that
   allocating memory failed.

   The memory will be released when the arena is destroyed.

.. function:: hyperdex_ds_allocate_attribute_check(arena, sz)

   Allocate an array of ``struct hyperdex_client_attribute_check``.

   :param arena: An arena allocated via ``hyperdex_ds_arena_create``
   :type arena: ``struct hyperdex_ds_arena``
   :param sz:  The number of elements in the array
   :type sz: ``size_t``
   :return: A newly allocated array
   :rtype: ``struct hyperdex_client_attribute_check*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_client_attribute_check*
      hyperdex_ds_allocate_attribute_check(struct hyperdex_ds_arena* arena, size_t sz);

   On success, this function returns a non-null pointer to containing ``sz``
   elements. On failure, the function returns ``NULL``, indicating that
   allocating memory failed.

   The memory will be released when the arena is destroyed.

.. function:: hyperdex_ds_allocate_map_attribute(arena, sz)

   Allocate an array of ``struct hyperdex_client_map_attribute``.

   :param arena: An arena allocated via ``hyperdex_ds_arena_create``
   :type arena: ``struct hyperdex_ds_arena``
   :param sz:  The number of elements in the array
   :type sz: ``size_t``
   :return: A newly allocated array
   :rtype: ``struct hyperdex_client_map_attribute*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_client_map_attribute*
      hyperdex_ds_allocate_map_attribute(struct hyperdex_ds_arena* arena, size_t sz);

   On success, this function returns a non-null pointer to containing ``sz``
   elements. On failure, the function returns ``NULL``, indicating that
   allocating memory failed.

   The memory will be released when the arena is destroyed.

Encoding Helper Functions
~~~~~~~~~~~~~~~~~~~~~~~~~

These functions help pack/unpack integers into the format required by HyperDex.

.. function:: hyperdex_ds_pack_int(num, buf)

   Pack ``num`` into the bytes pointed to by ``buf``.

   :param num: The number to pack
   :type num: ``int64_t``
   :param buf: A pointer to at least 8 bytes where ``num`` will be written
   :type buf: ``char*``

   The full prototype is:

   .. sourcecode:: c

      void
      hyperdex_ds_pack_int(int64_t num, char* value);

   This function always succeeds.

   It is the caller's responsibility to ensure that ``buf`` points to at least
   8 bytes.

.. function:: hyperdex_ds_unpack_int(buf, buf_sz, num)

   Unpack an integer from ``buf``/``buf_sz`` and store it in ``num``.

   :param buf: A bytestring to unpack from
   :type buf: ``const char*``
   :param buf_sz: The number of bytes pointed to by ``buf``
   :type buf_sz: ``size_t``
   :param num: Where to store the decoded integer
   :type num: ``int64_t*``
   :return: 0 on succes, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_unpack_int(const char* buf, size_t buf_sz, int64_t* num);

   This function will fail and return -1 if ``buf``/``buf_sz`` do not represent
   a valid integer.

These functions help pack/unpack floats into the format required by HyperDex.

.. function:: hyperdex_ds_pack_float(num, buf)

   Pack ``num`` into the bytes pointed to by ``buf``.

   :param num: The number to pack
   :type num: ``float``
   :param buf: A pointer to at least 8 bytes where ``num`` will be written
   :type buf: ``char*``

   The full prototype is:

   .. sourcecode:: c

      void
      hyperdex_ds_pack_float(double num, char* value);

   This function always succeeds.

   It is the caller's responsibility to ensure that ``buf`` points to at least
   8 bytes.

.. function:: hyperdex_ds_unpack_float(buf, buf_sz, num)

   Unpack a flot from ``buf``/``buf_sz`` and store it in ``num``.

   :param buf: A bytestring to unpack from
   :type buf: ``const char*``
   :param buf_sz: The number of bytes pointed to by ``buf``
   :type buf_sz: ``size_t``
   :param num: Where to store the decoded float
   :type num: ``double*``
   :return: 0 on succes, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_unpack_float(const char* buf, size_t buf_sz, double* num);

   This function will fail and return -1 if ``buf``/``buf_sz`` do not represent
   a valid double.

Copying Primitives
~~~~~~~~~~~~~~~~~~

.. function:: hyperdex_ds_copy_string(arena, str, str_sz, status, value, value_sz)

   Copy the string ``str``/``str_sz`` into a memory allocated via ``arena`` and
   return the copy via ``value`` and ``value_sz``.

   :param arena: Arena used for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :param str: The string to copy
   :type str: ``const char*``
   :param str_sz: Length of ``str``.
   :type str_sz: ``size_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: A pointer to the copied string
   :type value: ``const char**``
   :param value_sz: Length of ``value``.
   :type value_sz: ``size_t*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_copy_string(struct hyperdex_ds_arena* arena,
                              const char* str, size_t str_sz,
                              enum hyperdex_ds_returncode* status,
                              const char** value, size_t* value_sz);

   This function will fail and return -1 if there is insufficient memory
   available for copying the string.

   All pointers returned by this function will remain valid until ``arena`` is
   destroyed.

.. function:: hyperdex_ds_copy_int(arena, num, status, value, value_sz)

   Encode the integer ``num`` into memory allocated via ``arena`` and return the
   value via ``value`` and ``value_sz``.

   :param arena: Arena used for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :param num: The number to encode
   :type num: ``int64_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: A pointer to the encoded number
   :type value: ``const char**``
   :param value_sz: Length of ``value``.
   :type value_sz: ``size_t*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_copy_int(struct hyperdex_ds_arena* arena, int64_t num,
                           enum hyperdex_ds_returncode* status,
                           const char** value, size_t* value_sz);

   This function will fail and return -1 if there is insufficient memory
   available for encoding the number.

   All pointers returned by this function will remain valid until ``arena`` is
   destroyed.

   The values returned via ``value`` and ``value_sz`` represent the bytestring
   form of the integer, and are suitable for passing to the HyperDex client
   library.

.. function:: hyperdex_ds_copy_float(arena, num, status, value, value_sz)

   Encode the float ``num`` into memory allocated via ``arena`` and return the
   value via ``value`` and ``value_sz``.

   :param arena: Arena used for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :param num: The number to encode
   :type num: ``double``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: A pointer to the encoded number
   :type value: ``const char**``
   :param value_sz: Length of ``value``.
   :type value_sz: ``size_t*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_copy_float(struct hyperdex_ds_arena* arena, double num,
                             enum hyperdex_ds_returncode* status,
                             const char** value, size_t* value_sz);

   This function will fail and return -1 if there is insufficient memory
   available for encoding the number.

   All pointers returned by this function will remain valid until ``arena`` is
   destroyed.

   The values returned via ``value`` and ``value_sz`` represent the bytestring
   form of the float, and are suitable for passing to the HyperDex client
   library.

Building Lists
~~~~~~~~~~~~~~

HyperDex lists are encoded to byte strings.  Building the encoded form of a list
can lead to repetitive boilerplate -- especially when converting data types in
dynamic languages where the individual elements must be individually inspected.

The below functions obviate the need for such boilerplate by incrementally
building a list, performing all relevant error checking to ensure that the
resuling HyperDex list is well-formed.  The first element appended to the list
implicitly determines the type of the list.  All subsequent calls that push
elements of a different type will fail.

.. function:: hyperdex_ds_allocate_list(arena)

   Create a new dynamic list.

   :param arena: Arena to use for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :return: A new dynamic list
   :rtype: ``struct hyperdex_ds_list*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_ds_list*
      hyperdex_ds_allocate_list(struct hyperdex_ds_arena* arena);

   This function will fail and return a ``NULL`` pointer should memory
   allocation fail.

.. function:: hyperdex_ds_list_insert_string(list, str, str_sz, status)

   Append the string specified by ``str`` and ``str_sz`` to ``list``.

   :param list: List to which ``str`` will be appended
   :type list: ``struct hyperdex_ds_list*``
   :param str: String to append
   :type str: ``const char*``
   :param str_sz: Length of ``str``
   :type str_sz: ``size_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_list_insert_string(struct hyperdex_ds_list* list,
                                     const char* str, size_t str_sz,
                                     enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the list
   is not a list of strings.

.. function:: hyperdex_ds_list_insert_int(list, num, status)

   Append the integer ``num`` to ``list``.

   :param list: List to which ``num`` will be appended
   :type list: ``struct hyperdex_ds_list*``
   :param num: Integer to append
   :type num: ``const char*``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_list_insert_int(struct hyperdex_ds_list* list,
                                  int64_t num,
                                  enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the list
   is not a list of integers.

.. function:: hyperdex_ds_list_insert_float(list, num, status)

   Append the float ``num`` to ``list``.

   :param list: List to which ``num`` will be appended
   :type list: ``struct hyperdex_ds_list*``
   :param num: Float to append
   :type num: ``const char*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_list_insert_float(struct hyperdex_ds_list* list,
                                    double num,
                                    enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the list
   is not a list of floats.

.. function:: hyperdex_ds_list_finalize(list, status, value, value_sz, datatype)

   Finalize the list and return a bytestring and the list's datatype.

   :param list: List to finalize
   :type list: ``struct hyperdex_ds_list*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: Bytestring created by finalization
   :type value: ``const char**``
   :param value_sz:  Length of ``value``
   :type value_sz: ``size_t*``
   :param datatype: The type of the list
   :type datatype: ``enum hyperdatatype*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_list_finalize(struct hyperdex_ds_list*,
                                enum hyperdex_ds_returncode* status,
                                const char** value, size_t* value_sz,
                                enum hyperdatatype* datatype);

   This function will fail and return -1 if memory allocation fails.

Building Sets
~~~~~~~~~~~~~

HyperDex sets are encoded to byte strings.  Building the encoded form of a set
can lead to repetitive boilerplate -- especially when converting data types in
dynamic languages where the individual elements must be individually inspected.

The below functions obviate the need for such boilerplate by incrementally
building a set, performing all relevant error checking to ensure that the
resuling HyperDex set is well-formed and sorted.  The first element inserted
into the set implicitly determines the type of the set.  All subsequent calls
that insert elements of different types will fail.

.. function:: hyperdex_ds_allocate_set(arena)

   Create a new dynamic set.

   :param arena: Arena to use for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :return: A new dynamic set
   :rtype: ``struct hyperdex_ds_set*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_ds_set*
      hyperdex_ds_allocate_set(struct hyperdex_ds_arena* arena);

   This function will fail and return a ``NULL`` pointer should memory
   allocation fail.

.. function:: hyperdex_ds_set_insert_string(set, str, str_sz, status)

   Append the string specified by ``str`` and ``str_sz`` to ``set``.

   :param set: Set intto which ``str`` will be inserted
   :type set: ``struct hyperdex_ds_set*``
   :param str: String to append
   :type str: ``const char*``
   :param str_sz: Length of ``str``
   :type str_sz: ``size_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_set_insert_string(struct hyperdex_ds_set* set,
                                    const char* str, size_t str_sz,
                                    enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the set
   is not a set of strings.

.. function:: hyperdex_ds_set_insert_int(set, num, status)

   Append the integer ``num`` to ``set``.

   :param set: Set into which ``num`` will be inserted
   :type set: ``struct hyperdex_ds_set*``
   :param num: Integer to append
   :type num: ``const char*``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_set_insert_int(struct hyperdex_ds_set* set,
                                 int64_t num,
                                 enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the set
   is not a set of integers.

.. function:: hyperdex_ds_set_insert_float(set, num, status)

   Append the float ``num`` to ``set``.

   :param set: Set into which ``num`` will be inserted
   :type set: ``struct hyperdex_ds_set*``
   :param num: Float to append
   :type num: ``const char*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_set_insert_float(struct hyperdex_ds_set* set,
                                   double num,
                                   enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the set
   is not a set of floats.

.. function:: hyperdex_ds_set_finalize(set, status, value, value_sz, datatype)

   Finalize the set and return a bytestring and the set's datatype.

   :param set: Set to finalize
   :type set: ``struct hyperdex_ds_set*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: Bytestring created by finalization
   :type value: ``const char**``
   :param value_sz:  Length of ``value``
   :type value_sz: ``size_t*``
   :param datatype: The type of the set
   :type datatype: ``enum hyperdatatype*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_set_finalize(struct hyperdex_ds_set*,
                               enum hyperdex_ds_returncode* status,
                               const char** value, size_t* value_sz,
                               enum hyperdatatype* datatype);

   This function will fail and return -1 if memory allocation fails.

Building Maps
~~~~~~~~~~~~~

HyperDex maps are encoded to byte strings.  Building the encoded form of a map
can lead to repetitive boilerplate -- especially when converting data types in
dynamic languages where the individual elements must be individually inspected.

The below functions obviate the need for such boilerplate by incrementally
building a map, performing all relevant error checking to ensure that the
resuling HyperDex map is well-formed and sorted.  The first key/value-pair
inserted into the map implicitly determines the type of the map.  All subsequent
calls that insert elements of different types will fail.

The map is built by alternating calls to the key/value functions described
below, starting with a key-based function.  This keeps the number of cases
linear in the number of primitive types a map may contain, rather than appending
key-value pairs directly (which would require a quadratic number of calls).

.. function:: hyperdex_ds_allocate_map(arena)

   Create a new dynamic map.

   :param arena: Arena to use for memory allocation
   :type arena: ``struct hyperdex_ds_arena*``
   :return: A new dynamic map
   :rtype: ``struct hyperdex_ds_map*``

   The full prototype is:

   .. sourcecode:: c

      struct hyperdex_ds_map*
      hyperdex_ds_allocate_map(struct hyperdex_ds_arena* arena);

   This function will fail and return a ``NULL`` pointer should memory
   allocation fail.

.. function:: hyperdex_ds_map_insert_key_string(map, str, str_sz, status)

   Set the key of the next pair to be inserted into ``map`` to the string
   specified by ``str`` and ``str_sz``.

   :param map: Map for which ``str`` will be used as the key
   :type map: ``struct hyperdex_ds_map*``
   :param str: String to use as the key
   :type str: ``const char*``
   :param str_sz: Length of ``str``
   :type str_sz: ``size_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_key_string(struct hyperdex_ds_map* map,
                                        const char* str, size_t str_sz,
                                        enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not use strings as its key type.

.. function:: hyperdex_ds_map_insert_val_string(map, str, str_sz, status)

   Set the value of the next pair to be inserted into ``map`` to the string
   specified by ``str`` and ``str_sz``, and insert the pair.

   :param map: Map for which ``str`` will be used as the value
   :type map: ``struct hyperdex_ds_map*``
   :param str: String to use as the value
   :type str: ``const char*``
   :param str_sz: Length of ``str``
   :type str_sz: ``size_t``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_val_string(struct hyperdex_ds_map* map,
                                        const char* str, size_t str_sz,
                                        enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not use strings as its value type.

.. function:: hyperdex_ds_map_insert_key_int(map, num, status)

   Set the key of the next pair to be inserted into ``map`` to the integer
   specified by ``num``.

   :param map: Map for which ``num`` will be inserted
   :type map: ``struct hyperdex_ds_map*``
   :param num: Integer to use as the key
   :type num: ``const char*``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_key_int(struct hyperdex_ds_map* map,
                                     int64_t num,
                                     enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not uses integers as its key type.

.. function:: hyperdex_ds_map_insert_val_int(map, num, status)

   Set the value of the next pair to be inserted into ``map`` to the integer
   specified by ``num``, and insert the pair.

   :param map: Map for which ``num`` will be inserted
   :type map: ``struct hyperdex_ds_map*``
   :param num: Integer to use as the value
   :type num: ``const char*``
   :param status: Reason for failure (if any).
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_val_int(struct hyperdex_ds_map* map,
                                     int64_t num,
                                     enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not use integers as its value type.

.. function:: hyperdex_ds_map_insert_key_float(map, num, status)

   Set the key of the next pair to be inserted into ``map`` to the float
   specified by ``num``.

   :param map: Map for which ``num`` will be inserted
   :type map: ``struct hyperdex_ds_map*``
   :param num: Float to use as the key
   :type num: ``const char*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_key_float(struct hyperdex_ds_map* map,
                                       double num,
                                       enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not use floats as its key type.

.. function:: hyperdex_ds_map_insert_val_float(map, num, status)

   Set the value of the next pair to be inserted into ``map`` to the float
   specified by ``num``, and insert the pair.

   :param map: Map for which ``num`` will be inserted
   :type map: ``struct hyperdex_ds_map*``
   :param num: Float to use as the value
   :type num: ``const char*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_insert_val_float(struct hyperdex_ds_map* map,
                                       double num,
                                       enum hyperdex_ds_returncode* status);

   This function will fail and return -1 if memory allocation fails, or the map
   does not use floats as its value type.

.. function:: hyperdex_ds_map_finalize(map, status, value, value_sz, datatype)

   Finalize the map and return a bytestring and the map's datatype.

   :param map: Set to finalize
   :type map: ``struct hyperdex_ds_map*``
   :param status: Reason for failure (if any)
   :type status: ``enum hyperdex_ds_returncode*``
   :param value: Bytestring created by finalization
   :type value: ``const char**``
   :param value_sz:  Length of ``value``
   :type value_sz: ``size_t*``
   :param datatype: The type of the map
   :type datatype: ``enum hyperdatatype*``
   :return: 0 on success, -1 on failure

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_map_finalize(struct hyperdex_ds_map*,
                               enum hyperdex_ds_returncode* status,
                               const char** value, size_t* value_sz,
                               enum hyperdatatype* datatype);

   This function will fail and return -1 if memory allocation fails.

Iterating Over Containers
~~~~~~~~~~~~~~~~~~~~~~~~~

HyperDex provides an API for extracting elements from lists, sets, and maps.
These function calls return the elements one at a time with a minimal amount of
copying and allocation.  All iterators are used in the same pattern.  For
example, to iterate a list of integers:

.. literalinclude:: iterate.c
   :language: c

This code sample will output:

.. sourcecode:: console

   $ cc -o iterate iterate.c `pkg-config --cflags --libs hyperdex-client`
   $ ./iterate
   1
   -1
   3735928559

The function ``hyperdex_ds_iterator_init`` sets up the iterator.  For each
container datatype there is a specialized iteration function.  The coplete API
is:

.. function:: hyperdex_ds_iterator_init(iter, datatype, value, value_sz)

   Initialize an iterator for the given datatype/value.

   :param iter: Pointer to the iterator to initialize
   :type iter: ``struct hyperdex_ds_iterator*``
   :param datatype: Type of the value
   :type datatype: ``enum hyperdatatype``
   :param value: Value to iterate over
   :type value: ``const char*``
   :param value_sz: Number of bytes pointed to by ``value``
   :type value_sz: ``size_t``

   The full prototype is:

   .. sourcecode:: c

      void
      hyperdex_ds_iterator_init(struct hyperdex_ds_iterator* iter,
                                enum hyperdatatype datatype,
                                const char* value,
                                size_t value_sz);

   This function always succeeds.

.. function:: hyperdex_ds_iterate_list_string_next(iter, str, str_sz)

   Return the next string element in the list.

   :param iter: An iterator previously initialized to a list of strings
   :type iter: ``struct hyperdex_ds_iterator*``
   :param str: Where to store the returned string
   :type str: ``const char**``
   :param str_sz: Where to store the returned string's size
   :type str_sz: ``size_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_list_string_next(struct hyperdex_ds_iterator* iter,
                                           const char** str, size_t* str_sz);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the list of strings is malformed.

   The value stored in ``*str`` is a pointer into the list of strings and should
   not be free'd by the application.

.. function:: hyperdex_ds_iterate_list_int_next(iter, num)

   Return the next integer element in the list.

   :param iter: An iterator previously initialized to a list of integers
   :type iter: ``struct hyperdex_ds_iterator*``
   :param num: Where to store the returned number
   :type num: ``int64_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_list_int_next(struct hyperdex_ds_iterator* iter, int64_t* num);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the list of strings is malformed.

.. function:: hyperdex_ds_iterate_list_float_next(iter, num)

   Return the next floating point element in the list.

   :param iter: An iterator previously initialized to a list of floats
   :type iter: ``struct hyperdex_ds_iterator*``
   :param num: Where to store the returned number
   :type num: ``double*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_list_float_next(struct hyperdex_ds_iterator* iter, double* num);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the list of strings is malformed.

.. function:: hyperdex_ds_iterate_set_string_next(iter, str, str_sz)

   Return the next string element in the set.

   :param iter: An iterator previously initialized to a set of strings
   :type iter: ``struct hyperdex_ds_iterator*``
   :param str: Where to store the returned string
   :type str: ``const char**``
   :param str_sz: Where to store the returned string's size
   :type str_sz: ``size_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_set_string_next(struct hyperdex_ds_iterator* iter,
                                          const char** str, size_t* str_sz);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the set of strings is malformed.

   The value stored in ``*str`` is a pointer into the set of strings and should
   not be free'd by the application.

.. function:: hyperdex_ds_iterate_set_int_next(iter, num)

   Return the next integer element in the set.

   :param iter: An iterator previously initialized to a set of integers
   :type iter: ``struct hyperdex_ds_iterator*``
   :param num: Where to store the returned number
   :type num: ``int64_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_set_int_next(struct hyperdex_ds_iterator* iter, int64_t* num);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the set of ints is malformed.

.. function:: hyperdex_ds_iterate_set_float_next(iter, num)

   Return the next floating point element in the set.

   :param iter: An iterator previously initialized to a set of floats
   :type iter: ``struct hyperdex_ds_iterator*``
   :param num: Where to store the returned number
   :type num: ``double*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_set_float_next(struct hyperdex_ds_iterator* iter, double* num);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the set of floats is malformed.

.. function:: hyperdex_ds_iterate_map_string_string_next(iter, key, key_sz, val, val_sz)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from strings to strings
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``const char**``
   :param key_sz: Where to store the returned key's size
   :type key_sz: ``size_t*``
   :param val: Where to store the returned value
   :type val: ``const char**``
   :param val_sz: Where to store the returned value's size
   :type val_sz: ``size_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_string_string_next(struct hyperdex_ds_iterator* iter,
                                                 const char** key, size_t* key_sz,
                                                 const char** val, size_t* val_sz);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

   The values stored in ``*key`` and ``*val`` are pointers into the map and
   should not be free'd by the application.

.. function:: hyperdex_ds_iterate_map_string_int_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from strings to integers
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``const char**``
   :param key_sz: Where to store the returned key's size
   :type key_sz: ``size_t*``
   :param val: Where to store the returned value
   :type val: ``int64_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_string_int_next(struct hyperdex_ds_iterator* iter,
                                              const char** key, size_t* key_sz,
                                              int64_t* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

   The value stored in ``*key`` is a pointer into the map and should not be
   free'd by the application.

.. function:: hyperdex_ds_iterate_map_string_float_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from strings to floats
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``const char**``
   :param key_sz: Where to store the returned key's size
   :type key_sz: ``size_t*``
   :param val: Where to store the returned value
   :type val: ``double*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_string_float_next(struct hyperdex_ds_iterator* iter,
                                                const char** key, size_t* key_sz,
                                                double* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

   The value stored in ``*key`` is a pointer into the map and should not be
   free'd by the application.

.. function:: hyperdex_ds_iterate_map_int_string_next(iter, key, key_sz, val, val_sz)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from integers to strings
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``int64_t*``
   :param val: Where to store the returned value
   :type val: ``const char**``
   :param val_sz: Where to store the returned value's size
   :type val_sz: ``size_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_int_string_next(struct hyperdex_ds_iterator* iter,
                                              int64_t* key,
                                              const char** val, size_t* val_sz);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is is malformed.

   The value stored in ``*val`` is a pointer into the map and should not be
   free'd by the application.

.. function:: hyperdex_ds_iterate_map_int_int_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from integers to integers
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``int64_t*``
   :param val: Where to store the returned value
   :type val: ``int64_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_int_int_next(struct hyperdex_ds_iterator* iter,
                                           int64_t* key, int64_t* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

.. function:: hyperdex_ds_iterate_map_int_float_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from integers to floats
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``int64_t*``
   :param val: Where to store the returned value
   :type val: ``double*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_int_float_next(struct hyperdex_ds_iterator* iter,
                                             int64_t* key, double* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

.. function:: hyperdex_ds_iterate_map_float_string_next(iter, key, key_sz, val, val_sz)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from floats to strings
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``double*``
   :param val: Where to store the returned value
   :type val: ``const char**``
   :param val_sz: Where to store the returned value's size
   :type val_sz: ``size_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_float_string_next(struct hyperdex_ds_iterator* iter,
                                                double* key,
                                                const char** val, size_t* val_sz);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is is malformed.

   The value stored in ``*val`` is a pointer into the map and should not be
   free'd by the application.

.. function:: hyperdex_ds_iterate_map_float_int_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from floats to integers
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``double*``
   :param val: Where to store the returned value
   :type val: ``int64_t*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_float_int_next(struct hyperdex_ds_iterator* iter,
                                             double* key, int64_t* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.

.. function:: hyperdex_ds_iterate_map_float_float_next(iter, key, key_sz, val)

   Return the next key-value pair in the map.

   :param iter: An iterator previously initialized to a map from floats to floats
   :type iter: ``struct hyperdex_ds_iterator*``
   :param key: Where to store the returned key
   :type key: ``double*``
   :param val: Where to store the returned value
   :type val: ``double*``

   The full prototype is:

   .. sourcecode:: c

      int
      hyperdex_ds_iterate_map_float_float_next(struct hyperdex_ds_iterator* iter,
                                               double* key, double* val);

   This function will return 1 if an element is returned, 0 if there are no
   elements to return, and -1 if the map is malformed.
