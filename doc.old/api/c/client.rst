Client
------

The HyperDex Client library, ``libhyperdex-client`` is the de facto way to
access a HyperDex cluster for storing and retrieving data.  All data-store
operations are provided by the this library.

.. note:: Until release 1.0.rc5, ``libhyperdex-client`` was called
          ``libhyperclient`` it was changed to be more consistent with the
          naming of other HyperDex C libraries.

Building the HyperDex C Binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HyperDex C Binding is automatically built and installed via the normal
HyperDex build and install process.  You can ensure that the client is always
built by providing the ``--enable-client`` option to ``./configure`` like so:

.. sourcecode:: console

   $ ./configure --enable-client

Compiling and Linking Your Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unless otherwise noted, all Client operations are defined in the
``hyperdex/client.h`` include.  You can include this in your own program with:

.. sourcecode:: c

   #include <hyperdex/client.h>

To link against ``libhyperdex-client``, provide the ``-lhyperdex-client`` option
at link time:

.. sourcecode:: console

   $ cc -o output input.c -I/path/to/hyperdex/include -L/path/to/hyperdex/lib -lhyperdex-client

HyperDex provides support for the automatically determining the compiler and
linker flags for ``libhyperdex-client``.  First, ensure the ``pkg-config``
program is installed, and then run:

.. sourcecode:: console

   $ pkg-config --cflags hyperdex-client
   -I/usr/local/include
   $ pkg-config --libs hyperdex-client
   -L/usr/local/lib -lhyperdex-client

The first command outputs the compiler flags necessary to include the
``<hyperdex/client.h>`` file, while the second command outputs the flags
necessary for linking against ``libhyperdex-client``.

To put it all together, you can compile your application with:

.. sourcecode:: console

   $ cc -o output input.c `pkg-config --cflags --libs hyperdex-client`

.. seealso:: `Using pkg-config`_ for other common uses of ``pkg-config``.

.. _Using pkg-config: http://people.freedesktop.org/~dbn/pkg-config-guide.html#using

Hello World
~~~~~~~~~~~

The following is a minimal application that stores the value "Hello World" and
then immediately retrieves the value:

.. literalinclude:: hello-world.c
   :language: c

You can compile and run this example with:

.. sourcecode:: console

   $ cc -o hello-world hello-world.c `pkg-config --cflags --libs hyperdex-client`
   $ ./hello-world
   put "Hello World!"
   get done
   got attribute "v" = "Hello World!"

Right away, there are several points worth noting in this example:

 - Each operation, whether it is a PUT or a GET, is broken down into a request
   and a response.  The ``hyperdex_client_put`` and ``hyperdex_client_get``
   operations each initiate the request.  The application then calls
   ``hyperdex_client_loop`` to wait for the completion of the operation.

 - The stored value is passed via ``struct hyperdex_client_attribute`` which
   specifies both the value of the string "Hello World!" and that this value is,
   in fact, a string.  All HyperDex datatypes are passed to HyperDex as
   bytestrings via this interface.

 - This code omits error checking, assuming that all operations succeed.  In the
   subsequent documentation we'll explore proper error checking, which should be
   in place for all non-example code.

Asynchronous Patterns
~~~~~~~~~~~~~~~~~~~~~

All operations are issued *asynchronously*, that is, the operation's start is
decoupled from its completion, and it is up to the application to wait for its
completion.

The C API provides a unified, event-loop-like interface for polling events for
their completion.  An application can issue any number of operations, and poll
for their completion using the ``hyperdex_client_loop`` call.  This provides you
with several advantages unavailable in any other distributed key-value store:

 - Asynchronous events free your application to perform other operations while
   waiting for events to complete.

 - Asynchronous events re-use underlying resources, such as TCP connections to
   the cluster, across operations.  This enables improved performance and
   consumes fewer resources than would be consumed by synchronous clients
   generating a similar workload.

 - Operations will be buffered in userspace when they cannot be immediately sent
   to a server.  This ensures that applications will never block waiting for
   slow servers.

Of course, applications do not need to embrace this asynchronous design pattern.
It's always possible to use the library in a synchronous manner by immediately
following every operation with a call to ``hyperdex_client_loop``

Applications that do embrace this asynchronous design pattern will have a
certain structure.  Specifically:

 - Each operation must be followed by a call to ``hyperdex_client_loop``.  This
   is the core of the HyperDex client.  All work, including flushing userspace
   buffers, occurs via the call to loop.

 - Pointers used for output from operations must remain valid until
   ``hyperdex_client_loop`` indicates that the operation has successfully
   finished.  Consequently, they must not be aliased to pointers passed to other
   operations.

Error Handling
~~~~~~~~~~~~~~

Every call in the client provides a means for reporting failure.  After each
call, your application should for the error and react appropriately.  Depending
upon the error, your application may retry the request, or may need to take more
drastic action.

Errors are typically reported via ``enum hyperdex_client_returncode``.  This
enum is defined to be:

.. sourcecode:: c

   enum hyperdex_client_returncode
   {
       HYPERDEX_CLIENT_SUCCESS      = 8448,
       HYPERDEX_CLIENT_NOTFOUND     = 8449,
       HYPERDEX_CLIENT_SEARCHDONE   = 8450,
       HYPERDEX_CLIENT_CMPFAIL      = 8451,
       HYPERDEX_CLIENT_READONLY     = 8452,

       HYPERDEX_CLIENT_UNKNOWNSPACE = 8512,
       HYPERDEX_CLIENT_COORDFAIL    = 8513,
       HYPERDEX_CLIENT_SERVERERROR  = 8514,
       HYPERDEX_CLIENT_POLLFAILED   = 8515,
       HYPERDEX_CLIENT_OVERFLOW     = 8516,
       HYPERDEX_CLIENT_RECONFIGURE  = 8517,
       HYPERDEX_CLIENT_TIMEOUT      = 8519,
       HYPERDEX_CLIENT_UNKNOWNATTR  = 8520,
       HYPERDEX_CLIENT_DUPEATTR     = 8521,
       HYPERDEX_CLIENT_NONEPENDING  = 8523,
       HYPERDEX_CLIENT_DONTUSEKEY   = 8524,
       HYPERDEX_CLIENT_WRONGTYPE    = 8525,
       HYPERDEX_CLIENT_NOMEM        = 8526,
       HYPERDEX_CLIENT_BADCONFIG    = 8527,
       HYPERDEX_CLIENT_BADSPACE     = 8528,
       HYPERDEX_CLIENT_DUPLICATE    = 8529,
       HYPERDEX_CLIENT_INTERRUPTED  = 8530,
       HYPERDEX_CLIENT_CLUSTER_JUMP = 8531,
       HYPERDEX_CLIENT_COORD_LOGGED = 8532,
       HYPERDEX_CLIENT_OFFLINE      = 8533,

       HYPERDEX_CLIENT_INTERNAL     = 8573,
       HYPERDEX_CLIENT_EXCEPTION    = 8574,
       HYPERDEX_CLIENT_GARBAGE      = 8575
   };

In general, the first group of return codes are returned during normal operation
of the system.  They are:

``HYPERDEX_CLIENT_SUCCESS``
   The operation succeeded.  This will never be returned in an error condition.

``HYPERDEX_CLIENT_NOTFOUND``
   The requested object was not found.

``HYPERDEX_CLIENT_SEARCHDONE``
   An operation that potentially returns multiple objects (e.g.,
   ``hyperdex_client_search``) has finished and will no longer return from
   ``hyperdex_client_loop``.

``HYPERDEX_CLIENT_CMPFAIL``
   The predicate specified in the operation was not met.

``HYPERDEX_CLIENT_READONLY``
   The cluster is in read-only mode and not accepting write operations.

The following errors stem from environmental problems or problems within the
application.

``HYPERDEX_CLIENT_UNKNOWNSPACE``
   The specified space does not exist.

``HYPERDEX_CLIENT_COORDFAIL``
   The connection to the coordinator has failed and the application should
   back off before retrying.

``HYPERDEX_CLIENT_SERVERERROR``
   A server returned a nonsensical result to the client library.

``HYPERDEX_CLIENT_POLLFAILED``
   The poll system call failed in an unexpected manner.  Typically, this means
   that the application or HyperDex have mismanaged file descriptors, and that
   this error indicates a bug in one or both.

``HYPERDEX_CLIENT_OVERFLOW``
   An integer operation failed to complete because it would have resulted in
   signed overflow.

``HYPERDEX_CLIENT_RECONFIGURE``
   The server responsible for managing the operation failed while the operation
   was in-flight.

``HYPERDEX_CLIENT_TIMEOUT``
   The ``hyperdex_client_loop`` operation exceeded its timeout without
   completing an outstanding operation.

``HYPERDEX_CLIENT_UNKNOWNATTR``
   The operation references an attribute that is not part of the referenced
   space.

``HYPERDEX_CLIENT_DUPEATTR``
   The operation references an attribute multiple times in a way that results in
   undefined behavior.

``HYPERDEX_CLIENT_NONEPENDING``
   The ``hyperdex_client_loop`` call was called with no outstanding operations.

``HYPERDEX_CLIENT_DONTUSEKEY``
   The key was used as a mutable attribute, or as part of a search.

``HYPERDEX_CLIENT_WRONGTYPE``
   An attribute or predicate uses a type that is not compatible with the type of
   the referenced space.

``HYPERDEX_CLIENT_NOMEM``
   HyperDex failed to allocate memory.

``HYPERDEX_CLIENT_BADCONFIG``
   The coordinator has sent a faulty configuration.  This will never happen
   except in the presence of bugs.

``HYPERDEX_CLIENT_DUPLICATE``
   The meta-data operation would duplicate or reinitialize some of the cluster
   meta-data.

``HYPERDEX_CLIENT_INTERRUPTED``
   The HyperDex library was interrupted by a signal.  Read more about `Working
   with Signals`_.

``HYPERDEX_CLIENT_CLUSTER_JUMP``
   The coordinator is issuing configurations for a new cluster.  This will
   happen when a coordinator is deployed to replace an existing coordinator at
   the same address.  This is not an error that affects the HyperDex client
   library, just a notification to the application.

``HYPERDEX_CLIENT_COORD_LOGGED``
   There was an error that was logged on the coordinator nodes.  It's likely
   sensitive in nature, so further information is unavailable.

``HYPERDEX_CLIENT_OFFLINE``
   All servers responsible for handling the specified operation are currently
   offline and unavailable, whether due to failure or planned downtime.

The following errors indicate significant bugs within the client or application.

``HYPERDEX_CLIENT_INTERNAL``
   One or more of the HyperDex client library's internal invariants have been
   broken.  It's best to destroy and recreate the client.

``HYPERDEX_CLIENT_EXCEPTION``
   The C library is implemented internally using C++.  C++ generated an
   unhandled exception that was caught at the C boundary.  This indicates a
   serious bug in HyperDex, and exists only as a safeguard.  Applications should
   never see this error.

``HYPERDEX_CLIENT_GARBAGE``
   This value is reserved as a well-defined value that the library will never
   return, and that is not used as a constant anywhere else within HyperDex.

Note that an asynchronous application should distinguish between *local*
errors which affect one outstanding operation, and *global* errors that
transiently affect all operations, but do not change the completion status of
those operations.

Local errors are always returned via the ``enum hyperdex_client_returncode*``
pointer passed at the time the application initated the operation.  These errors
may either result from the application returning a negative operation id, in
which case the operation immediately completes, or from the result of a
successful ``hyperdex_client_loop`` call.  In either case, the error has no
impact on the result of any other operation.

Global errors are always returned via the ``enum hyperdex_client_returncode*``
pointer passed to ``hyperdex_client_loop``.  These errors are not localized to
any particular operation and indicate errors that are non-permanent.  Example
global errors include application-requested timeouts, interruptions by signals,
and temporary non-connectivity with the coordinator.

``struct hyperdex_client``
~~~~~~~~~~~~~~~~~~~~~~~~~~

A HyperDex client is encapsulated within the incomplete ``struct
hyperdex_client`` type.

.. function:: hyperdex_client_create(coordinator, port)

   Create a new HyperDex client

   :param coordinator: The IP address or hostname of the coordinator
   :param port: The port of the coordinator
   :rtype: ``struct hyperdex_client*``

   The C prototype for this call is:

   .. sourcecode:: c

      struct hyperdex_client*
      hyperdex_client_create(const char* coordinator, uint16_t port);

   This call allocates and initializes local structures.  If it returns a null
   pointer, the global error variable ``errno`` will indicate why the operation
   could not complete.

   .. note:: This call only allocates local data structure, and does not
             initialize the connection to the coordinator.  It will be
             automatically initialized, and subsequently maintained,
             by the normal operations on the client instance.

.. function:: hyperdex_client_destroy(client)

   Destroy a HyperDex client and all its associated resources

   :param client: The client to destroy.

   The C prototype for this call is:

   .. sourcecode:: c

      void
      hyperdex_client_destroy(struct hyperdex_client* client);

   This call will always succeed.

``struct hyperdex_client_attribute``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

HyperDex operations that must associate a value with an attribute of an object
use ``struct hyperdex_client_attribute``.  In the most basic case of
``hyperdex_client_put``, this struct specifies the attribute (via the ``attr``
member), and it's value (via the remaining three members).  For more complex
operations, such as ``hyperdex_client_string_prepend``, the value serves as an
argument to the operation -- in the case of prepend, it is the value to be
prepended.

Its C definition is:

.. sourcecode:: c

   struct hyperdex_client_attribute
   {
       const char* attr; /* NULL-terminated */
       const char* value;
       size_t value_sz;
       enum hyperdatatype datatype;
   };

``struct hyperdex_client_map_attribute``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some HyperDex operations on maps require specifying the attribute that is the
map, the key of the map, and the value of the that key within the map.

Its C definition is:

.. sourcecode:: c

   struct hyperdex_client_map_attribute
   {
       const char* attr; /* NULL-terminated */
       const char* map_key;
       size_t map_key_sz;
       enum hyperdatatype map_key_datatype;
       const char* value;
       size_t value_sz;
       enum hyperdatatype value_datatype;
   };

``struct hyperdex_client_attribute_check``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All predicates used for conditional operations or specifying search parameters
are passed using ``struct hyperdex_client_attribute_check``.

Its C definition is:

.. sourcecode:: c

   struct hyperdex_client_attribute_check
   {
       const char* attr; /* NULL-terminated */
       const char* value;
       size_t value_sz;
       enum hyperdatatype datatype;
       enum hyperpredicate predicate;
   };

Client Operations
~~~~~~~~~~~~~~~~~

.. function:: hyperdex_client_get(client, space, key, key_sz, status, attrs, attrs_sz)

   .. include:: ../shards/get.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :param attrs: Where to store the attributes of the retrieved object
   :type attrs: ``const struct hyperdex_client_attribute**``
   :param attrs_sz: Where to store the number of retrieved attributes
   :type attrs_sz: ``size_t*``
   :return success: A non-negative integer ID 
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_get(struct hyperdex_client* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          enum hyperdex_client_returncode* status,
                          const struct hyperdex_client_attribute** attrs, size_t* attrs_sz);

   .. include:: ../shards/get.long.rst

.. function:: hyperdex_client_put(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/put.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to store, overwriting preexisting values.
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_put(struct hyperdex_client* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/put.long.rst

.. function:: hyperdex_client_cond_put(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_put.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to store, overwriting preexisting values.
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_put(struct hyperdex_client* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                               const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                               enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_put.long.rst

.. function:: hyperdex_client_put_if_not_exist(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/put_if_not_exist.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to store
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_put_if_not_exist(struct hyperdex_client* client,
                                       const char* space,
                                       const char* key, size_t key_sz,
                                       const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                       enum hyperdex_client_returncode* status);

   .. include:: ../shards/put_if_not_exist.long.rst

.. function:: hyperdex_client_del(client, space, key, key_sz, status)

   .. include:: ../shards/del.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_del(struct hyperdex_client* client,
                          const char* space,
                          const char* key, size_t key_sz,
                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/del.long.rst

.. function:: hyperdex_client_cond_del(client, space, key, key_sz, checks, checks_sz, status)

   .. include:: ../shards/cond_del.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_del(struct hyperdex_client* client,
                               const char* space,
                               const char* key, size_t key_sz,
                               const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                               enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_del.long.rst

.. function:: hyperdex_client_atomic_add(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be added
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_add(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_add.long.rst

.. function:: hyperdex_client_cond_atomic_add(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be added
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_add(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_add.long.rst

.. function:: hyperdex_client_atomic_sub(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_sub.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_sub(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_sub.long.rst

.. function:: hyperdex_client_cond_atomic_sub(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_sub.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_sub(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_sub.long.rst

.. function:: hyperdex_client_atomic_mul(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_mul.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes by which the specified values will be multiplied
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_mul(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_mul.long.rst

.. function:: hyperdex_client_cond_atomic_mul(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_mul.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_mul(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_mul.long.rst

.. function:: hyperdex_client_atomic_div(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_div.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes by which the specified values will be divided
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_div(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_div.long.rst

.. function:: hyperdex_client_cond_atomic_div(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_div.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes by which the specified values will be divided
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_div(struct hyperdex_client* client,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                    const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_div.long.rst

.. function:: hyperdex_client_atomic_mod(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_mod.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes which will be taken modulo the specified value
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_mod(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_mod.long.rst

.. function:: hyperdex_client_cond_atomic_mod(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_mod.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes which will be taken modulo the specified value
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_mod(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_mod.long.rst

.. function:: hyperdex_client_atomic_and(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_and.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-anded with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_and(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_and.long.rst

.. function:: hyperdex_client_cond_atomic_and(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_and.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-and'ed with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_and(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_and.long.rst

.. function:: hyperdex_client_atomic_or(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_or.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-or'ed with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_or(struct hyperdex_client* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_or.long.rst

.. function:: hyperdex_client_cond_atomic_or(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_or.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-or'ed with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_or(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_or.long.rst

.. function:: hyperdex_client_atomic_xor(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/atomic_xor.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-xor'ed with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_atomic_xor(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/atomic_xor.long.rst

.. function:: hyperdex_client_cond_atomic_xor(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_atomic_xor.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes which will be bitwise-xor'ed with the specified values
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_atomic_xor(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_atomic_xor.long.rst

.. function:: hyperdex_client_string_prepend(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/string_prepend.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be prepended
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_string_prepend(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/string_prepend.long.rst

.. function:: hyperdex_client_cond_string_prepend(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_string_prepend.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be prepended
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_string_prepend(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_string_prepend.long.rst

.. function:: hyperdex_client_string_append(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/string_append.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be appended
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_string_append(struct hyperdex_client* client,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    enum hyperdex_client_returncode* status);

   .. include:: ../shards/string_append.long.rst

.. function:: hyperdex_client_cond_string_append(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_string_append.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be appended
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_string_append(struct hyperdex_client* client,
                                         const char* space,
                                         const char* key, size_t key_sz,
                                         const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                         const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                         enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_string_append.long.rst

.. function:: hyperdex_client_list_lpush(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/list_lpush.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be left-pushed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_list_lpush(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/list_lpush.long.rst

.. function:: hyperdex_client_cond_list_lpush(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_list_lpush.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be left-pushed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_list_lpush(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_list_lpush.long.rst

.. function:: hyperdex_client_list_rpush(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/list_rpush.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be right-pushed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_list_rpush(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/list_rpush.long.rst

.. function:: hyperdex_client_cond_list_rpush(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_list_rpush.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be right-pushed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_list_rpush(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_list_rpush.long.rst

.. function:: hyperdex_client_set_add(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/set_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be added
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_set_add(struct hyperdex_client* client,
                              const char* space,
                              const char* key, size_t key_sz,
                              const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                              enum hyperdex_client_returncode* status);

   .. include:: ../shards/set_add.long.rst

.. function:: hyperdex_client_cond_set_add(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_set_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes to which the specified values will be added
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_set_add(struct hyperdex_client* client,
                                   const char* space,
                                   const char* key, size_t key_sz,
                                   const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                   enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_set_add.long.rst

.. function:: hyperdex_client_set_remove(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/set_remove.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes from which the specified values will be removed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_set_remove(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/set_remove.long.rst

.. function:: hyperdex_client_cond_set_remove(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_set_remove.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes from which the specified values will be removed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_set_remove(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_set_remove.long.rst

.. function:: hyperdex_client_set_intersect(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/set_intersect.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes with which the specified values will be intersected
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_set_intersect(struct hyperdex_client* client,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                    enum hyperdex_client_returncode* status);

   .. include:: ../shards/set_intersect.long.rst

.. function:: hyperdex_client_cond_set_intersect(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_set_intersect.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes with which the specified values will be intersected
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_set_intersect(struct hyperdex_client* client,
                                         const char* space,
                                         const char* key, size_t key_sz,
                                         const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                         const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                         enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_set_intersect.long.rst

.. function:: hyperdex_client_set_union(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/set_union.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes with which the specified values will be unioned
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_set_union(struct hyperdex_client* client,
                                const char* space,
                                const char* key, size_t key_sz,
                                const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                enum hyperdex_client_returncode* status);

   .. include:: ../shards/set_union.long.rst

.. function:: hyperdex_client_cond_set_union(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_set_union.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes with which the specified values will be unioned
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_set_union(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                     const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_set_union.long.rst

.. function:: hyperdex_client_map_add(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes to which the specified keys will be added
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_add(struct hyperdex_client* client,
                              const char* space,
                              const char* key, size_t key_sz,
                              const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                              enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_add.long.rst

.. function:: hyperdex_client_cond_map_add(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The attributes to which the specified keys will be added
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_add(struct hyperdex_client* client,
                                   const char* space,
                                   const char* key, size_t key_sz,
                                   const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                   enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_add.long.rst

.. function:: hyperdex_client_map_remove(client, space, key, key_sz, attrs, attrs_sz, status)

   .. include:: ../shards/map_remove.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param attrs: The attributes from which the specified keys will be removed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_remove(struct hyperdex_client* client,
                                 const char* space,
                                 const char* key, size_t key_sz,
                                 const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                 enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_remove.long.rst

.. function:: hyperdex_client_cond_map_remove(client, space, key, key_sz, checks, checks_sz, attrs, attrs_sz, status)

   .. include:: ../shards/cond_map_remove.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param attrs: The attributes from which the specified keys will be removed
   :type attrs: ``const struct hyperdex_client_attribute*``
   :param attrs_sz: The number of attributes pointed to by ``attrs``
   :type attrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_remove(struct hyperdex_client* client,
                                      const char* space,
                                      const char* key, size_t key_sz,
                                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const struct hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_remove.long.rst

.. function:: hyperdex_client_map_atomic_add(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be added
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_add(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_add.long.rst

.. function:: hyperdex_client_cond_map_atomic_add(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_add.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be added
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_add(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_add.long.rst

.. function:: hyperdex_client_map_atomic_sub(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_sub.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values from which the specified values will be removed
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_sub(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_sub.long.rst

.. function:: hyperdex_client_cond_map_atomic_sub(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_sub.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values from which the specified values will be removed
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_sub(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_sub.long.rst

.. function:: hyperdex_client_map_atomic_mul(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_mul.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values by which the specified values will be multiplied
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_mul(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_mul.long.rst

.. function:: hyperdex_client_cond_map_atomic_mul(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_mul.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values by which the specified values will be multiplied
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_mul(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_mul.long.rst

.. function:: hyperdex_client_map_atomic_div(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_div.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values by which the specified values will be divided
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_div(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_div.long.rst

.. function:: hyperdex_client_cond_map_atomic_div(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_div.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values by which the specified values will be divided
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_div(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_div.long.rst

.. function:: hyperdex_client_map_atomic_mod(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_mod.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values which will be taken modulo the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_mod(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_mod.long.rst

.. function:: hyperdex_client_cond_map_atomic_mod(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_mod.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values which will be taken modulo the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_mod(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_mod.long.rst

.. function:: hyperdex_client_map_atomic_and(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_and.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-and'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_and(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_and.long.rst

.. function:: hyperdex_client_cond_map_atomic_and(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_and.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-and'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_and(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_and.long.rst

.. function:: hyperdex_client_map_atomic_or(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_or.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-or'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_or(struct hyperdex_client* client,
                                    const char* space,
                                    const char* key, size_t key_sz,
                                    const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                    enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_or.long.rst

.. function:: hyperdex_client_cond_map_atomic_or(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_or.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-or'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_or(struct hyperdex_client* client,
                                         const char* space,
                                         const char* key, size_t key_sz,
                                         const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                         const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                         enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_or.long.rst

.. function:: hyperdex_client_map_atomic_xor(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_atomic_xor.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-xor'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_atomic_xor(struct hyperdex_client* client,
                                     const char* space,
                                     const char* key, size_t key_sz,
                                     const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                     enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_atomic_xor.long.rst

.. function:: hyperdex_client_cond_map_atomic_xor(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_atomic_xor.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values which will be bitwise-xor'ed with the specified values
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_atomic_xor(struct hyperdex_client* client,
                                          const char* space,
                                          const char* key, size_t key_sz,
                                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                          const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                          enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_atomic_xor.long.rst

.. function:: hyperdex_client_map_string_prepend(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_string_prepend.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be prepended
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_string_prepend(struct hyperdex_client* client,
                                         const char* space,
                                         const char* key, size_t key_sz,
                                         const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                         enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_string_prepend.long.rst

.. function:: hyperdex_client_cond_map_string_prepend(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_string_prepend.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be prepended
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_string_prepend(struct hyperdex_client* client,
                                              const char* space,
                                              const char* key, size_t key_sz,
                                              const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                              const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                              enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_string_prepend.long.rst

.. function:: hyperdex_client_map_string_append(client, space, key, key_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/map_string_append.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be appended
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_map_string_append(struct hyperdex_client* client,
                                        const char* space,
                                        const char* key, size_t key_sz,
                                        const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                        enum hyperdex_client_returncode* status);

   .. include:: ../shards/map_string_append.long.rst

.. function:: hyperdex_client_cond_map_string_append(client, space, key, key_sz, checks, checks_sz, mapattrs, mapattrs_sz, status)

   .. include:: ../shards/cond_map_string_append.short.rst

   :param client: Initialized client
   :type client: ``struct hyperdex_client*``
   :param space: Space of the object referenced by ``key``
   :type space: ``const char*``
   :param key: Bytestring representation of the object's key
   :type key: ``const char*``
   :param key_sz: Number of bytes in ``key``
   :type key_sz: ``size_t``
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: ``const struct hyperdex_client_attribute_check*``
   :param checks_sz: The number of attributes pointed to by ``checks``
   :type checks_sz: ``size_t``
   :param mapattrs: The keys' values to which the specified values will be appended
   :type mapattrs: ``const struct hyperdex_client_map_attribute*``
   :param mapattrs_sz: The number of attributes pointed to by ``mapattrs``
   :type mapattrs_sz: ``size_t``
   :param status: Where to store the result of the operation
   :type status: ``enum hyperdex_client_returncode*``
   :return success: A non-negative integer ID
   :return failure: A negative number

   The full prototype is:

   .. sourcecode:: c

      int64_t
      hyperdex_client_cond_map_string_append(struct hyperdex_client* client,
                                             const char* space,
                                             const char* key, size_t key_sz,
                                             const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                             const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                             enum hyperdex_client_returncode* status);

   .. include:: ../shards/cond_map_string_append.long.rst

int64_t
hyperdex_client_search(struct hyperdex_client* client,
                       const char* space,
                       const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                       enum hyperdex_client_returncode* status,
                       const struct hyperdex_client_attribute** attrs, size_t* attrs_sz);

int64_t
hyperdex_client_search_describe(struct hyperdex_client* client,
                                const char* space,
                                const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                                enum hyperdex_client_returncode* status,
                                const char** description);

int64_t
hyperdex_client_sorted_search(struct hyperdex_client* client,
                              const char* space,
                              const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                              const char* sort_by,
                              uint64_t limit,
                              int maxmin,
                              enum hyperdex_client_returncode* status,
                              const struct hyperdex_client_attribute** attrs, size_t* attrs_sz);

int64_t
hyperdex_client_group_del(struct hyperdex_client* client,
                          const char* space,
                          const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                          enum hyperdex_client_returncode* status);

int64_t
hyperdex_client_count(struct hyperdex_client* client,
                      const char* space,
                      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
                      enum hyperdex_client_returncode* status,
                      uint64_t* count);

int64_t
hyperdex_client_loop(struct hyperdex_client* client, int timeout,
                     enum hyperdex_client_returncode* status);

enum hyperdatatype
hyperdex_client_attribute_type(struct hyperdex_client* client,
                               const char* space, const char* name,
                               enum hyperdex_client_returncode* status);

const char*
hyperdex_client_error_message(struct hyperdex_client* client);

const char*
hyperdex_client_returncode_to_string(enum hyperdex_client_returncode);

void
hyperdex_client_destroy_attrs(const struct hyperdex_client_attribute* attrs, size_t attrs_sz);

Working with Signals
~~~~~~~~~~~~~~~~~~~~

The HyperDex client library provides a simple mechanism to cleanly integrate
with applications that work with signals.

Your application must mask all signals prior to making any calls into the
library.  The library will unmask the signals during blocking operations and
return ``HYPERDEX_CLIENT_INTERRUPTED`` should any signals be received.

Working with Threads
~~~~~~~~~~~~~~~~~~~~

The HyperDex client library is fully reentrant.  Instances of ``struct
hyperdex_client`` and their associated state may be accessed from multiple
threads, provided that the application provides its own synchronization that
provides mutual exclusion.

Put simply, a multi-threaded application should protect each ``struct
hyperdex_client`` instance with a mutex or lock to ensure correct operation.
