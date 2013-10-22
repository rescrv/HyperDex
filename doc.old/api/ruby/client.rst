Client
------

The HyperDex Client Ruby module, ``HyperDex::Client`` provides access to the
HyperDex client library from Ruby.  This module manages all datatype conversion
to convert between Ruby's and HyperDex's supported types.

.. note:: Until release 1.0.rc5, ``HyperDex::Client`` was called
          ``HyperClient`` it was changed to be more consistent with the naming
          of other HyperDex Ruby modules.

Building the HyperDex Ruby Binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HyperDex Ruby binding may be built and installed via the normal HyperDex
build and install process.  It is built when the ``--enable-ruby-bindings``
option is passed to ``./configure`` like so:

.. sourcecode:: console

   $ ./configure --enable-ruby bindings

Integrating With Your Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use HyperDex from ruby, require the ``'hyperdex'`` module like so:

.. sourcecode:: ruby

   require 'hyperdex'

Hello World
~~~~~~~~~~~

The following is a minimal application that stores the value "Hello World" and
then immediately retrieves the value:

.. literalinclude:: hello-world.rb
   :language: ruby

You can run this example with:

.. sourcecode:: console

   $ ruby hello-world.rb
   XXX

Right away, there are several points worth noting in this example:

 - Ruby operations are synchronous by default.  The ``put`` and the ``get`` in
   the example implicitly wait for the operation to complete.  The Ruby bindings
   provide ``async_put`` and ``async_get`` to enable asynchronous applications.

 - Our datatypes are automatically converted into HyperDex datattypes.

 - Space names and attribute names may be symbols or strings.  We find it more
   readable to make them symbols, but there's no technical requirement to do so.

 - This code shows no error checking.  The Ruby bindings will raise an exception
   of type ``HyperDex::Client::HyperDexClientException`` on error.

HyperDexClientException
~~~~~~~~~~~~~~~~~~~~~~~

Attributes
~~~~~~~~~~

Map Attributes
~~~~~~~~~~~~~~

Predicates
~~~~~~~~~~

Client
~~~~~~

.. method:: get(space, key)

   .. include:: ../shards/get.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type

   .. include:: ../shards/get.long.rst

.. method:: put(space, key, attrs)

   .. include:: ../shards/put.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to store, overwriting preexisting values.
   :type attrs: hash of attributes

   .. include:: ../shards/put.long.rst

.. method:: cond_put(space, key, checks, attrs)

   .. include:: ../shards/cond_put.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to store, overwriting preexisting values.
   :type attrs: hash of attributes

   .. include:: ../shards/cond_put.long.rst

.. method:: put_if_not_exist(space, key, attrs)

   .. include:: ../shards/put_if_not_exist.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to store
   :type attrs: hash of attributes

   .. include:: ../shards/put_if_not_exist.long.rst

.. method:: del(space, key)

   .. include:: ../shards/del.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type

   .. include:: ../shards/del.long.rst

.. method:: cond_del(space, key, checks)

   .. include:: ../shards/cond_del.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates

   .. include:: ../shards/cond_del.long.rst

.. method:: atomic_add(space, key, attrs)

   .. include:: ../shards/atomic_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be added
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_add.long.rst

.. method:: cond_atomic_add(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be added
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_add.long.rst

.. method:: atomic_sub(space, key, attrs)

   .. include:: ../shards/atomic_sub.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_sub.long.rst

.. method:: cond_atomic_sub(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_sub.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_sub.long.rst

.. method:: atomic_mul(space, key, attrs)

   .. include:: ../shards/atomic_mul.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes by which the specified values will be multiplied
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_mul.long.rst

.. method:: cond_atomic_mul(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_mul.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes from which the specified values will be subtracted
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_mul.long.rst

.. method:: atomic_div(space, key, attrs)

   .. include:: ../shards/atomic_div.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes by which the specified values will be divided
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_div.long.rst

.. method:: cond_atomic_div(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_div.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes by which the specified values will be divided
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_div.long.rst

.. method:: atomic_mod(space, key, attrs)

   .. include:: ../shards/atomic_mod.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes which will be taken modulo the specified value
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_mod.long.rst

.. method:: cond_atomic_mod(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_mod.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes which will be taken modulo the specified value
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_mod.long.rst

.. method:: atomic_and(space, key, attrs)

   .. include:: ../shards/atomic_and.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes which will be bitwise-anded with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_and.long.rst

.. method:: cond_atomic_and(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_and.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes which will be bitwise-and'ed with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_and.long.rst

.. method:: atomic_or(space, key, attrs)

   .. include:: ../shards/atomic_or.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes which will be bitwise-or'ed with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_or.long.rst

.. method:: cond_atomic_or(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_or.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes which will be bitwise-or'ed with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_or.long.rst

.. method:: atomic_xor(space, key, attrs)

   .. include:: ../shards/atomic_xor.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes which will be bitwise-xor'ed with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/atomic_xor.long.rst

.. method:: cond_atomic_xor(space, key, checks, attrs)

   .. include:: ../shards/cond_atomic_xor.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes which will be bitwise-xor'ed with the specified values
   :type attrs: hash of attributes

   .. include:: ../shards/cond_atomic_xor.long.rst

.. method:: string_prepend(space, key, attrs)

   .. include:: ../shards/string_prepend.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be prepended
   :type attrs: hash of attributes

   .. include:: ../shards/string_prepend.long.rst

.. method:: cond_string_prepend(space, key, checks, attrs)

   .. include:: ../shards/cond_string_prepend.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be prepended
   :type attrs: hash of attributes

   .. include:: ../shards/cond_string_prepend.long.rst

.. method:: string_append(space, key, attrs)

   .. include:: ../shards/string_append.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be appended
   :type attrs: hash of attributes

   .. include:: ../shards/string_append.long.rst

.. method:: cond_string_append(space, key, checks, attrs)

   .. include:: ../shards/cond_string_append.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be appended
   :type attrs: hash of attributes

   .. include:: ../shards/cond_string_append.long.rst

.. method:: list_lpush(space, key, attrs)

   .. include:: ../shards/list_lpush.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be left-pushed
   :type attrs: hash of attributes

.. method:: cond_list_lpush(space, key, checks, attrs)

   .. include:: ../shards/cond_list_lpush.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be left-pushed
   :type attrs: hash of attributes

   .. include:: ../shards/cond_list_lpush.long.rst

.. method:: list_rpush(space, key, attrs)

   .. include:: ../shards/list_rpush.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be right-pushed
   :type attrs: hash of attributes

   .. include:: ../shards/list_rpush.long.rst

.. method:: cond_list_rpush(space, key, checks, attrs)

   .. include:: ../shards/cond_list_rpush.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be right-pushed
   :type attrs: hash of attributes

   .. include:: ../shards/cond_list_rpush.long.rst

.. method:: set_add(space, key, attrs)

   .. include:: ../shards/set_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified values will be added
   :type attrs: hash of attributes

   .. include:: ../shards/set_add.long.rst

.. method:: cond_set_add(space, key, checks, attrs)

   .. include:: ../shards/cond_set_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes to which the specified values will be added
   :type attrs: hash of attributes

   .. include:: ../shards/cond_set_add.long.rst

.. method:: set_remove(space, key, attrs)

   .. include:: ../shards/set_remove.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes from which the specified values will be removed
   :type attrs: hash of attributes

   .. include:: ../shards/set_remove.long.rst

.. method:: cond_set_remove(space, key, checks, attrs)

   .. include:: ../shards/cond_set_remove.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes from which the specified values will be removed
   :type attrs: hash of attributes

   .. include:: ../shards/cond_set_remove.long.rst

.. method:: set_intersect(space, key, attrs)

   .. include:: ../shards/set_intersect.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes with which the specified values will be intersected
   :type attrs: hash of attributes

   .. include:: ../shards/set_intersect.long.rst

.. method:: cond_set_intersect(space, key, checks, attrs)

   .. include:: ../shards/cond_set_intersect.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes with which the specified values will be intersected
   :type attrs: hash of attributes

   .. include:: ../shards/cond_set_intersect.long.rst

.. method:: set_union(space, key, attrs)

   .. include:: ../shards/set_union.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes with which the specified values will be unioned
   :type attrs: hash of attributes

   .. include:: ../shards/set_union.long.rst

.. method:: cond_set_union(space, key, checks, attrs)

   .. include:: ../shards/cond_set_union.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes with which the specified values will be unioned
   :type attrs: hash of attributes

   .. include:: ../shards/cond_set_union.long.rst

.. method:: map_add(space, key, mapattrs)

   .. include:: ../shards/map_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes to which the specified keys will be added
   :type attrs: hash of attributes

   .. include:: ../shards/map_add.long.rst

.. method:: cond_map_add(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The attributes to which the specified keys will be added
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_add.long.rst

.. method:: map_remove(space, key, attrs)

   .. include:: ../shards/map_remove.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param attrs: The attributes from which the specified keys will be removed
   :type attrs: hash of attributes

   .. include:: ../shards/map_remove.long.rst

.. method:: cond_map_remove(space, key, checks, attrs)

   .. include:: ../shards/cond_map_remove.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param attrs: The attributes from which the specified keys will be removed
   :type attrs: hash of attributes

   .. include:: ../shards/cond_map_remove.long.rst

.. method:: map_atomic_add(space, key, mapattrs)

   .. include:: ../shards/map_atomic_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values to which the specified values will be added
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_add.long.rst

.. method:: cond_map_atomic_add(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_add.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values to which the specified values will be added
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_add.long.rst

.. method:: map_atomic_sub(space, key, mapattrs)

   .. include:: ../shards/map_atomic_sub.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values from which the specified values will be removed
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_sub.long.rst

.. method:: cond_map_atomic_sub(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_sub.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values from which the specified values will be removed
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_sub.long.rst

.. method:: map_atomic_mul(space, key, mapattrs)

   .. include:: ../shards/map_atomic_mul.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values by which the specified values will be multiplied
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_mul.long.rst

.. method:: cond_map_atomic_mul(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_mul.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values by which the specified values will be multiplied
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_mul.long.rst

.. method:: map_atomic_div(space, key, mapattrs)

   .. include:: ../shards/map_atomic_div.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values by which the specified values will be divided
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_div.long.rst

.. method:: cond_map_atomic_div(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_div.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values by which the specified values will be divided
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_div.long.rst

.. method:: map_atomic_mod(space, key, mapattrs)

   .. include:: ../shards/map_atomic_mod.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values which will be taken modulo the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_mod.long.rst

.. method:: cond_map_atomic_mod(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_mod.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values which will be taken modulo the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_mod.long.rst

.. method:: map_atomic_and(space, key, mapattrs)

   .. include:: ../shards/map_atomic_and.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values which will be bitwise-and'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_and.long.rst

.. method:: cond_map_atomic_and(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_and.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values which will be bitwise-and'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_and.long.rst

.. method:: map_atomic_or(space, key, mapattrs)

   .. include:: ../shards/map_atomic_or.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values which will be bitwise-or'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_or.long.rst

.. method:: cond_map_atomic_or(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_or.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values which will be bitwise-or'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_or.long.rst

.. method:: map_atomic_xor(space, key, mapattrs)

   .. include:: ../shards/map_atomic_xor.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values which will be bitwise-xor'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_atomic_xor.long.rst

.. method:: cond_map_atomic_xor(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_atomic_xor.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values which will be bitwise-xor'ed with the specified values
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_atomic_xor.long.rst

.. method:: map_string_prepend(space, key, mapattrs)

   .. include:: ../shards/map_string_prepend.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param mapattrs: The keys' values to which the specified values will be prepended
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_string_prepend.long.rst

.. method:: cond_map_string_prepend(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_string_prepend.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values to which the specified values will be prepended
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_string_prepend.long.rst

.. method:: map_string_append(space, key, mapattrs)

   .. include:: ../shards/map_string_append.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values to which the specified values will be appended
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/map_string_append.long.rst

.. method:: cond_map_string_append(space, key, checks, mapattrs)

   .. include:: ../shards/cond_map_string_append.short.rst

   :param space: Space of the object referenced by ``key``
   :type space: symbol or string
   :param key: The object's key
   :type key: key type
   :param checks: The predicates that must be true for the operation to succeed.
   :type checks: hash of predicates
   :param mapattrs: The keys' values to which the specified values will be appended
   :type mapattrs: hash of map keys and values

   .. include:: ../shards/cond_map_string_append.long.rst

Working with Signals
~~~~~~~~~~~~~~~~~~~~

The HyperDex client bindings provide a simple mechanism to cleanly integrate
with applications that work with signals.

Your application must mask all signals prior to making any calls into the
library.  The library will unmask the signals during blocking operations and
raise a ``HYPERDEX_CLIENT_INTERRUPTED`` error should any signals be received.

Working with Threads
~~~~~~~~~~~~~~~~~~~~

The HyperDex client library is fully reentrant.  Instances of
``HyperDex::Client::Client`` and their associated state may be accessed from
multiple threads, provided that the application provides its own synchronization
that provides mutual exclusion.

Put simply, a multi-threaded application should protect each ``Client`` instance
and its associated objects with a mutex or lock to ensure correct operation.
