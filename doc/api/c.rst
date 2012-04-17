C/C++ API
=========

The C API provides programmers with the most efficient way to access a HyperDex
cluster.  For this reason, the C API is used to implement scripting language
APIs, and high performance applications.  Where applicable, C++ names for
functions are indicated.  We ask that all custom benchmarks utilize the C API
when other systems use native APIs as well.


Types
~~~~~
.. c:type:: hyperclient

   An opaque type containing all information about a single HyperDex client.
   An instance of :c:type:`hyperclient` will maintain connections to the
   HyperDex coordinator, and HyperDex daemons.

   This class exists in C++ as well.

   .. seealso:: :c:func:`hyperclient_create()`,
                :c:func:`hyperclient_destroy()`


.. c:type:: hyperdatatype

   An enum indicating the type of a HyperDex attribute.  Valid values are:

   ``HYPERDATATYPE_STRING``:
      The data is a byte string consisting of uninterpreted data.

   ``HYPERDATATYPE_INT64``:
      The data is interpreted as a signed, 64-bit little-endian integer.
      HyperDex will ignore trailing NULL bytes (they don't impact the integer's
      number), and will ignore any bytes beyond the first 8.

   ``HYPERDATATYPE_GARBAGE``:
      There was an internal error which leaves the API unable to interpret the
      type of an object.  This indicates an internal error within the HyperDex
      client library which should be fixed.

   The C++ API uses this type as-is.


.. c:type:: hyperclient_attribute

   A struct representing one attribute of an object.  Each attribute corresponds
   to one dimension in the hyperspace.  The members of this struct are:

   .. c:member:: const char* hyperclient_attribute.attr

      A NULL-terminated C-string containing the human-readable name assigned to
      the attribute at subspace creation.

   .. c:member:: const char* hyperclient_attribute.value

      The bytes to be used for the attribute.  These bytes will be interpreted
      by HyperDex according to the :c:data:`datatype` field.  This field is
      not NULL-terminated because its length is defined in :c:data:`value_sz`.

   .. c:member:: size_t hyperclient_attribute.value_sz

      The number of bytes pointed to by :c:data:`value`.

   .. c:member:: hyperclient_datatype hyperclient_attribute.datatype

      The manner in which HyperDex should interpret the contents of
      :c:data:`value`.  This field must match the type specified for
      :c:data:`attr` at the time the space was defined.

   The C++ API uses this type as-is.


.. c:type:: hyperclient_range_query

   A struct representing one element of a range query.  The members of this
   struct are:

   .. c:member:: const char* hyperclient_range_query.attr

      A NULL-terminated C-string containing the human-readable name assigned to
      the attribute at subspace creation.

   .. c:member:: uint64_t hyperclient_range_query.lower

      The lower bound on objects matching the range query.  All objects
      which match the query will have :c:data:`attr` be greater than or equal to
      :c:data:`lower`.

   .. c:member:: uint64_t hyperclient_range_query.upper

      The upper bound on objects matching the range query.  All objects
      which match the query will have :c:data:`attr` be strictly less than
      :c:data:`upper`.

   The C++ API uses this type as-is.

   .. seealso:: :c:func:`hyperclient_search()`


.. c:type:: hyperclient_returncode

   An enum indicating the result of an operation.  Valid values are:

   ``HYPERCLIENT_SUCCESS``:
      The operation completed successfully.  For operations which return
      objects, there is an object to be read.

   ``HYPERCLIENT_NOTFOUND``:
      The operation completed successfully, but there was no object to return or
      operate on.

   ``HYPERCLIENT_SEARCHDONE``:
      The indicated search operation has completed.

      .. seealso:: :c:func:`hyperclient_search`

   ``HYPERCLIENT_CMPFAIL``:
      A conditional operation failed its comparison.

      .. seealso:: :c:func:`hyperclient_condput`

   ``HYPERCLIENT_UNKNOWNSPACE``:
      The space specified does not exist.

   ``HYPERCLIENT_COORDFAIL``:
      The client library has lost contact with the coordinator.

   ``HYPERCLIENT_SERVERERROR``:
      A server has malfunctioned.  This indicates the existence of a bug.

   ``HYPERCLIENT_CONNECTFAIL``:
      A connection to a server has failed.

   ``HYPERCLIENT_DISCONNECT``:
      A server has disconnected from the client library.

   ``HYPERCLIENT_RECONFIGURE``:
      The operation failed because the host contacted for the operation changed
      identities part way through the operation.

   ``HYPERCLIENT_LOGICERROR``:
      This indicates a bug in the client library.

   ``HYPERCLIENT_TIMEOUT``:
      The requested operation has exceeded its timeout, and is returning without
      completing any work.

      .. seealso:: :c:func:`hyperclient_loop`

   ``HYPERCLIENT_UNKNOWNATTR``:
      The attribute is not one of the attributes which define the space.

      .. seealso:: :c:member:`hyperclient_attribute.attr`

   ``HYPERCLIENT_DUPEATTR``:
      An attribute is specified twice.

      .. seealso:: :c:member:`hyperclient_attribute.attr`

   ``HYPERCLIENT_SEEERRNO``:
      The :c:data:`errno` variable will have more information about the failure.

   ``HYPERCLIENT_NONEPENDING``:
      Loop was called, but no operations are pending.

      .. seealso:: :c:func:`hyperclient_loop`

   ``HYPERCLIENT_DONTUSEKEY``:
      The key was used as part of an equality search.  The result of the search
      will be equivalent to a ``GET``, so a get should be performed instead.

      .. seealso:: :c:func:`hyperclient_get`,
                   :c:func:`hyperclient_search`

   ``HYPERCLIENT_WRONGTYPE``:
      There was a type mismatch.

      .. seealso:: :c:member:`hyperclient_attribute.datatype`

   ``HYPERCLIENT_EXCEPTION``:
      The underlying C code threw an exception that was masked to prevent it
      from propagating up a C call stack.  This indicates a bug in the client
      library.

   ``HYPERCLIENT_ZERO``:
      A value usable for testing for uninitialized variables.

   ``HYPERCLIENT_A``:
      A value usable for testing for uninitialized variables.

   ``HYPERCLIENT_B``:
      A value usable for testing for uninitialized variables.

   The C++ API uses this type as-is.


Functions
~~~~~~~~~

.. c:function:: hyperclient* hyperclient_create(const char* coordinator, in_port_t port)

   Return a :c:type:`hyperclient` instance.  The caller is responsible for
   releasing all resources held by this instance by calling
   :c:func:`hyperclient_destroy`.

   coordinator:
      A string containing the IP of the coordinator for the HyperDex cluster.

   port:
      A number containing the port of the coordinator for the HyperDex cluster.

   The C++ API provides a constructor for :c:type:`hyperclient` which takes the
   same arguments and performs the same functionality.

   .. seealso:: :c:type:`hyperclient`,
                :c:func:`hyperclient_destroy`


.. c:function:: void hyperclient_destroy(struct hyperclient* client)

   Free all resources associated with the :c:type:`hyperclient` pointed to by
   :c:data:`client`.

   client:
      The :c:type:`hyperclient` instance to be freed.

   The C++ API provides a destructor for :c:type:`hyperclient` in place of this
   call.

   .. seealso:: :c:type:`hyperclient`,
                :c:func:`hyperclient_create`


.. c:function:: int64_t hyperclient_get(struct hyperclient* client, const char* space, const char* key, size_t key_sz, enum hyperclient_returncode* status, struct hyperclient_attribute** attrs, size_t* attrs_sz)

   .. include:: shards/get.rst

   On success, the integer returned will be a positive integer unique to this
   request.  The request will be considered complete when
   :c:func:`hyperclient_loop` returns the same ID.  If the integer returned is
   negative, it indicates an error generating the request, and ``*status``
   contains the reason why.

   client:
      An initialized :c:type:`hyperclient` instance.

   space:
      A NULL-terminated C-string containing the name of the space to retrieve
      the object from.

   key:
      A sequence of bytes to be used as the key.  This pointer must remain valid
      for the duration of the call.

   key_sz:
      The number of bytes pointed to by :c:data:`key`.

   status:
      A return value in which the result of the operation will be stored.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.

   attrs:
      A return value in which the object (if any) will be stored.  This value
      will be changed if and only if ``*status`` is ``HYPERCLIENT_SUCCESS``.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.
      This returned value must be released with a call to
      :c:func:`hyperclient_destroy_attrs`.

      .. seealso:: :c:type:`hyperclient_attribute`,
                   :c:func:`hyperclient_destroy_attrs`

   attrs_sz:
      The number of :c:type:`hyperclient_attribute` instances returned in
      :c:data:`attrs`.  This value will be changed if and only if ``*status`` is
      ``HYPERCLIENT_SUCCESS``.  If this function returns successfully, this
      pointer must remain valid until :c:func:`hyperclient_loop` returns the
      same ID returned by this function.

   The C++ API provides ``hyperclient::get`` in addition to this call.


.. c:function:: int64_t hyperclient_put(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/put.rst

   On success, the integer returned will be a positive integer unique to this
   request.  The request will be considered complete when
   :c:func:`hyperclient_loop` returns the same ID.  If the integer returned is
   negative, it indicates an error generating the request, and ``*status``
   contains the reason why.  ``HYPERCLIENT_UNKNOWNATTR``,
   ``HYPERCLIENT_WRONGTYPE`` and ``HYPERCLIENT_DUPEATTR`` indicate which
   attribute caused the error by returning ``-1 - idx_of_bad_attr``.

   client:
      An initialized :c:type:`hyperclient` instance.

   space:
      A NULL-terminated C-string containing the name of the space to retrieve
      the object from.

   key:
      A sequence of bytes to be used as the key.  This pointer must remain valid
      for the duration of the call.

   key_sz:
      The number of bytes pointed to by :c:data:`key`.

   attrs:
      The attributes to be changed on the object.  This pointer must remain
      valid for the duration of the call.

   attrs_sz:
      THe number of attributes pointed to by :c:data:`attrs`.

   status:
      A return value in which the result of the operation will be stored.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.

   The C++ API provides ``hyperclient::put`` in place of this call.


.. c:function:: int64_t hyperclient_condput(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* condattrs, size_t condattrs_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/condput.rst

   On success, the integer returned will be a positive integer unique to this
   request.  The request will be considered complete when
   :c:func:`hyperclient_loop` returns the same ID.  If the integer returned is
   negative, it indicates an error generating the request, and ``*status``
   contains the reason why.  ``HYPERCLIENT_UNKNOWNATTR``,
   ``HYPERCLIENT_WRONGTYPE`` and ``HYPERCLIENT_DUPEATTR`` indicate which
   attribute caused the error by returning ``-1 - idx_of_bad_attr``.

   client:
      An initialized :c:type:`hyperclient` instance.

   space:
      A NULL-terminated C-string containing the name of the space to retrieve
      the object from.

   key:
      A sequence of bytes to be used as the key.  This pointer must remain valid
      for the duration of the call.

   key_sz:
      The number of bytes pointed to by :c:data:`key`.

   condattrs:
      The attributes to be compared against.  All attributes must match for the
      CONDITIONAL PUT to be successful.  This pointer must remain valid for the
      duration of the call.

   condattrs_sz:
      THe number of attributes pointed to by :c:data:`condattrs`.

   attrs:
      The attributes to be changed on the object.  This pointer must remain
      valid for the duration of the call.

   attrs_sz:
      THe number of attributes pointed to by :c:data:`attrs`.

   status:
      A return value in which the result of the operation will be stored.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.

   The C++ API provides ``hyperclient::condput`` in place of this call.


.. c:function:: int64_t hyperclient_del(struct hyperclient* client, const char* space, const char* key, size_t key_sz, enum hyperclient_returncode* status)

   .. include:: shards/del.rst

   On success, the integer returned will be a positive integer unique to this
   request.  The request will be considered complete when
   :c:func:`hyperclient_loop` returns the same ID.  If the integer returned is
   negative, it indicates an error generating the request, and ``*status``
   contains the reason why.

   client:
      An initialized :c:type:`hyperclient` instance.

   space:
      A NULL-terminated C-string containing the name of the space to retrieve
      the object from.

   key:
      A sequence of bytes to be used as the key.  This pointer must remain valid
      for the duration of the call.

   key_sz:
      The number of bytes pointed to by :c:data:`key`.

   status:
      A return value in which the result of the operation will be stored.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.

   The C++ API provides ``hyperclient::del`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_add(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_add.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_add`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_sub(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_sub.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_sub`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_mul(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_mul.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_mul`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_div(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_div.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_div`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_mod(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_mod.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_mod`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_and(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_and.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_and`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_or(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_or.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_or`` in place of this call.


.. c:function:: int64_t hyperclient_atomic_xor(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_xor.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::atomic_xor`` in place of this call.


.. c:function:: int64_t hyperclient_string_prepend(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/string_prepend.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::string_prepend`` in place of this call.


.. c:function:: int64_t hyperclient_string_append(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/string_append.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::string_append`` in place of this call.


.. c:function:: int64_t hyperclient_list_lpush(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/list_lpush.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::list_lpush`` in place of this call.


.. c:function:: int64_t hyperclient_list_rpush(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/list_rpush.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::list_rpush`` in place of this call.


.. c:function:: int64_t hyperclient_set_add(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/set_add.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::set_add`` in place of this call.


.. c:function:: int64_t hyperclient_set_remove(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/set_remove.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::set_remove`` in place of this call.


.. c:function:: int64_t hyperclient_set_intersect(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/set_intersect.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::set_intersect`` in place of this call.


.. c:function:: int64_t hyperclient_set_union(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/set_union.rst
   .. include:: shards/cstandard_call.rst

   The C++ API provides ``hyperclient::set_union`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_add(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_add.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_add`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_sub(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_sub.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_sub`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_mul(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_mul.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_mul`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_div(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_div.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_div`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_mod(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_mod.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_mod`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_and(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_and.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_and`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_or(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_or.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_or`` in place of this call.


.. c:function:: int64_t hyperclient_map_atomic_xor(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/atomic_xor.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_atomic_xor`` in place of this call.


.. c:function:: int64_t hyperclient_map_string_prepend(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/string_prepend.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_string_prepend`` in place of this call.


.. c:function:: int64_t hyperclient_map_string_append(struct hyperclient* client, const char* space, const char* key, size_t key_sz, const struct hyperclient_map_attribute* attrs, size_t attrs_sz, enum hyperclient_returncode* status)

   .. include:: shards/string_append.rst
   .. include:: shards/cmap_call.rst

   The C++ API provides ``hyperclient::map_string_append`` in place of this call.


.. c:function:: int64_t hyperclient_search(struct hyperclient* client, const char* space, const struct hyperclient_attribute* eq, size_t eq_sz, const struct hyperclient_range_query* rn, size_t rn_sz, enum hyperclient_returncode* status, struct hyperclient_attribute** attrs, size_t* attrs_sz)

   .. include:: shards/search.rst

   The search must match attributes specified by :c:data:`eq` and :c:data:`rn`.
   On success, the integer returned will be a positive integer unique to this
   request.  One object or error is returned on each subsequent invocation of
   :c:func:`hyperclient_loop` which returns the same ID.  When
   :c:func:`hyperclient_loop` set ``*status`` to ``HYPERCLIENT_SEARCHDONE``, the
   search is complete.  If the integer returned is negative, it indicates an
   error generating the request, and ``*status`` contains the reason why.
   ``HYPERCLIENT_UNKNOWNATTR``, ``HYPERCLIENT_WRONGTYPE`` and
   ``HYPERCLIENT_DUPEATTR`` indicate which attribute caused the error by
   returning ``-1 - idx_of_bad_attr``, where ``idx_of_bad_attr`` is an index to
   into the combined attributes of :c:data:`eq` and :c:data:`rn`.

   client:
      An initialized :c:type:`hyperclient` instance.

   space:
      A NULL-terminated C-string containing the name of the space to retrieve
      the object from.

   eq:
      The attributes to which must match under a strict equality check.  This
      pointer must remain valid for the duration of the call.

   eq_sz:
      The number of attributes pointed to by :c:data:`eq`.

   rn:
      The attributes to which must fall within the range specified by
      :c:member:`hyperclient_range_query.lower` and :c:member:`hyperclient_range_query.upper`

      .. seealso:: :c:type:`hyperclient_range_query`

   rn_sz:
      The number of attributes pointed to by :c:data:`rn`.

   status:
      A return value in which the result of the operation will be stored.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function.

   attrs:
      A return value in which the object (if any) will be stored.  This value
      will be changed if and only if ``*status`` is ``HYPERCLIENT_SUCCESS``.  If
      this function returns successfully, this pointer must remain valid until
      :c:func:`hyperclient_loop` returns the same ID returned by this function
      with ``*status`` set to ``HYPERCLIENT_SEARCHDONE``.  This returned value
      must be released with a call to :c:func:`hyperclient_destroy_attrs`.

      This value will be overwritten each time :c:func:`hyperclient_loop`
      returns, and the value will not be saved.  It is up to the caller of this
      function to either destroy the attributes, or preserve a pointer to them
      before calling :c:func:`hyperclient_loop` again.

      .. seealso:: :c:type:`hyperclient_attribute`,
                   :c:func:`hyperclient_destroy_attrs`

   attrs_sz:
      The number of :c:type:`hyperclient_attribute` instances returned in
      :c:data:`attrs`.  This value will be changed if and only if ``*status`` is
      ``HYPERCLIENT_SUCCESS``.  If this function returns successfully, this
      pointer must remain valid until :c:func:`hyperclient_loop` returns the
      same ID returned by this function with ``*status`` set to
      ``HYPERCLIENT_SEARCHDONE``.

   The C++ API provides ``hyperclient::search`` in place of this call.


.. c:function:: int64_t hyperclient_loop(struct hyperclient* client, int timeout, enum hyperclient_returncode* status)

   .. include:: shards/loop.rst

   The return value is a 64-bit integer which identifies the outstanding
   operation that was processed.  If an error is encountered or the event loop
   times out when processing the outstanding operations, the return value will
   be -1, and ``*status`` will be set to indicate the reason why.

   client:
      An initialized :c:type:`hyperclient` instance.

   timeout:
      The number of milliseconds to wait before giving up on event processing.

   status:
      A return value in which the result of the operation will be stored.  This
      pointer must remain valid for the duration of the call.

   The C++ API provides ``hyperclient::loop`` in place of this call.


.. c:function:: void hyperclient_destroy_attrs(struct hyperclient_attribute* attrs, size_t attrs_sz)

   Free an array of :c:type:`hyperclient_attribute` objects returned via the
   ``hyperclient`` API.  The returned values are allocated in a custom manner,
   and it is incorrect to free the memory using any other means.


Thread Safety
~~~~~~~~~~~~~

The HyperClient API uses no global or per-thread state.  All state is enclosed
in :c:type:`hyperclient` instances.  Concurrent access to the same
:c:type:`hyperclient` instance must be protected by external synchronization.
The C++ API requires that all calls to members of :c:type:`hyperclient` be
protected with external synchronization.
