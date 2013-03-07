Python API
==========

.. py:module:: hyperclient
   :synopsis: The Python HyperDex client

.. py:class:: Client(address, port)

   A client of the HyperDex cluster.  Instances of this class encapsulate all
   resources necessary to communicate with nodes in a HyperDex cluster.

   .. py:method:: get(space, key)

      .. include:: shards/get.rst

      On success, the returned object will be a ``dict`` mapping attribute names
      to their respective values.  If the object does not exist, ``None`` will
      be returned.  On error, a :py:exc:`HyperClientException` is thrown.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

   .. py:method:: put(space, key, attrs)

      .. include:: shards/put.rst

      The return value is a boolean indicating success.  On error, a
      :py:exc:`HyperClientException` is thrown.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to their respective values.  Each
         value must match the type defined when :py:obj:`space` was created.

   .. py:method:: cond_put(space, key, condition, attrs)

      .. include:: shards/cond_put.rst

      The return value is a boolean indicating success.  If the value is
      ``True``, the comparison was successful, and the object was updated.  If
      the value is ``False``, the comparison failed, and the object remains
      unchanged.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      condition:
         A dictionary mapping attribute names to a value which is compared
         against the object's current state.

      attrs:
         A dictionary mapping attribute names to their respective values.  Each
         value must match the type defined when :py:obj:`space` was created.

   .. py:method:: delete(space, key)

      .. include:: shards/del.rst

      The return value is a boolean indicating success.  If the value is
      ``True``, the delete was successful, and the object was removed.  If the
      value is ``False``, there was no object to remove.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

   .. py:method:: search(space, predicate)

      .. include:: shards/search.rst

      This object returns a generator of type :py:class:`Search`.  Both objects
      retrieved by the search, and errors encountered during the search will be
      yielded by the generator.  Each object is a ``dict`` mapping attribute
      names to their respective values.

      space:
         A string naming the space in which the object will be inserted.

      predicate:
         A dictionary specifying comparisons used for selecting objects.  Each
         key-value pair in :py:obj:`predicate` maps the name of an attribute to
         a value or range of values which constitute the search.  An equality
         search is specified by supplying the value to match.  A range search is
         a 2-tuple specifying the lower and upper bounds on the range.

   .. py:method:: sorted_search(space, predicate):

      .. include:: shards/sorted_search.rst

      This object returns a generator of type :py:class:`Search`.  Both objects
      retrieved by the search, and errors encountered during the search will be
      yielded by the generator.  Each object is a ``dict`` mapping attribute
      names to their respective values.

      space:
         A string naming the space in which the object will be inserted.

      predicate:
         A dictionary specifying comparisons used for selecting objects.  Each
         key-value pair in :py:obj:`predicate` maps the name of an attribute to
         a value or range of values which constitute the search.  An equality
         search is specified by supplying the value to match.  A range search is
         a 2-tuple specifying the lower and upper bounds on the range.

   .. py:method:: count(space, predicate):

      .. include:: shards/count.rst

      This object returns an integer (the count) or raises an exception.

      space:
         A string naming the space in which the object will be inserted.

      predicate:
         A dictionary specifying comparisons used for selecting objects.  Each
         key-value pair in :py:obj:`predicate` maps the name of an attribute to
         a value or range of values which constitute the search.  An equality
         search is specified by supplying the value to match.  A range search is
         a 2-tuple specifying the lower and upper bounds on the range.

   .. py:method:: group_del(space, predicate):

      .. include:: shards/group_del.rst

      This call returns nothing.

      space:
         A string naming the space in which the object will be inserted.

      predicate:
         A dictionary specifying comparisons used for selecting objects.  Each
         key-value pair in :py:obj:`predicate` maps the name of an attribute to
         a value or range of values which constitute the search.  An equality
         search is specified by supplying the value to match.  A range search is
         a 2-tuple specifying the lower and upper bounds on the range.

   .. py:method:: atomic_add(space, key, value)

      .. include:: shards/atomic_add.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_sub(space, key, value)

      .. include:: shards/atomic_sub.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_mul(space, key, value)

      .. include:: shards/atomic_mul.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_div(space, key, value)

      .. include:: shards/atomic_div.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_mod(space, key, value)

      .. include:: shards/atomic_mod.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_and(space, key, value)

      .. include:: shards/atomic_and.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_or(space, key, value)

      .. include:: shards/atomic_or.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: atomic_xor(space, key, value)

      .. include:: shards/atomic_xor.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: string_prepend(space, key, value)

      .. include:: shards/string_prepend.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: string_append(space, key, value)

      .. include:: shards/string_append.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: list_lpush(space, key, value)

      .. include:: shards/list_lpush.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: list_rpush(space, key, value)

      .. include:: shards/list_rpush.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: set_add(space, key, value)

      .. include:: shards/set_add.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: set_remove(space, key, value)

      .. include:: shards/set_remove.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: set_intersect(space, key, value)

      .. include:: shards/set_intersect.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: set_union(space, key, value)

      .. include:: shards/set_intersect.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: map_atomic_add(space, key, value)

      .. include:: shards/atomic_add.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_sub(space, key, value)

      .. include:: shards/atomic_sub.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_mul(space, key, value)

      .. include:: shards/atomic_mul.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_div(space, key, value)

      .. include:: shards/atomic_div.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_mod(space, key, value)

      .. include:: shards/atomic_mod.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_and(space, key, value)

      .. include:: shards/atomic_and.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_or(space, key, value)

      .. include:: shards/atomic_or.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_atomic_xor(space, key, value)

      .. include:: shards/atomic_xor.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_string_prepend(space, key, value)

      .. include:: shards/string_prepend.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: map_string_append(space, key, value)

      .. include:: shards/string_append.rst
      .. include:: shards/pytruefalse.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_get(space, key)

      .. include:: shards/get.rst

      The returned object will be a :py:class:`DeferredGet` instance which
      tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

   .. py:method:: async_put(space, key, value)

      .. include:: shards/put.rst

      The returned object will be a :py:class:`DeferredPut` instance which
      tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to their respective values.  Each
         value must match the type defined when :py:obj:`space` was created.

   .. py:method:: async_cond_put(space, key, condition, value)

      .. include:: shards/cond_put.rst

      The returned object will be a :py:class:`DeferredCondput` instance which
      tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      condition:
         A dictionary mapping attribute names to a value which is compared
         against the object's current state.

      attrs:
         A dictionary mapping attribute names to their respective values.  Each
         value must match the type defined when :py:obj:`space` was created.

   .. py:method:: async_delete(space, key)

      .. include:: shards/del.rst

      The returned object will be a :py:class:`DeferredDelete` instance which
      tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

   .. py:method:: async_atomic_add(space, key, value)

      .. include:: shards/atomic_add.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_sub(space, key, value)

      .. include:: shards/atomic_sub.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_mul(space, key, value)

      .. include:: shards/atomic_mul.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_div(space, key, value)

      .. include:: shards/atomic_div.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_mod(space, key, value)

      .. include:: shards/atomic_mod.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_and(space, key, value)

      .. include:: shards/atomic_and.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_or(space, key, value)

      .. include:: shards/atomic_or.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_atomic_xor(space, key, value)

      .. include:: shards/atomic_xor.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_string_prepend(space, key, value)

      .. include:: shards/string_prepend.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_string_append(space, key, value)

      .. include:: shards/string_append.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_list_lpush(space, key, value)

      .. include:: shards/list_lpush.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_list_rpush(space, key, value)

      .. include:: shards/list_rpush.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_set_add(space, key, value)

      .. include:: shards/set_add.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_set_remove(space, key, value)

      .. include:: shards/set_remove.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_set_intersect(space, key, value)

      .. include:: shards/set_intersect.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: async_map_atomic_add(space, key, value)

      .. include:: shards/atomic_add.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_sub(space, key, value)

      .. include:: shards/atomic_sub.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_mul(space, key, value)

      .. include:: shards/atomic_mul.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_div(space, key, value)

      .. include:: shards/atomic_div.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_mod(space, key, value)

      .. include:: shards/atomic_mod.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_and(space, key, value)

      .. include:: shards/atomic_and.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_or(space, key, value)

      .. include:: shards/atomic_or.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_atomic_xor(space, key, value)

      .. include:: shards/atomic_xor.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_string_prepend(space, key, value)

      .. include:: shards/string_prepend.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_map_string_append(space, key, value)

      .. include:: shards/string_append.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pymap_args.rst

   .. py:method:: async_set_union(space, key, value)

      .. include:: shards/set_intersect.rst
      .. include:: shards/pydeferred.rst
      .. include:: shards/pystandard_args.rst

   .. py:method:: loop()

      .. include:: shards/loop.rst

      The returned object will be a :py:class:`Deferred` instance which tracks
      the request.  The object will allow the user to immediately call
      :py:meth:`wait` without blocking.

.. py:class:: HyperClientException(status, attr)

   An exception that describes an error within the HyperClient library.

   .. py:method:: status()

      A numeric error code indicating the issue.  This will be one of the
      ``HYPERCLIENT_*`` error codes.

.. py:class:: DeferredGet

   .. py:method:: wait()

      Wait for the operation to complete.  On success, the returned object will
      be a ``dict`` mapping attribute names to their respective values.  If the
      object does not exist, ``None`` will be returned.  On error, a
      :py:exc:`HyperClientException` is thrown.

.. py:class:: DeferredPut

   .. py:method:: wait()

      Wait for the operation to complete.  The return value is a boolean
      indicating success.  On error, a :py:exc:`HyperClientException` is thrown.

.. py:class:: DeferredCondPut

   .. py:method:: wait()

      Wait for the operation to complete.  The return value is a boolean
      indicating success.  If the value is ``True``, the comparison was
      successful, and the object was updated.  If the value is ``False``, the
      comparison failed, and the object remains unchanged.

.. py:class:: DeferredCondPut

   .. py:method:: wait()

      Wait for the operation to complete.  The return value is a boolean
      indicating success.  If the value is ``True``, the delete was successful,
      and the object was removed.  If the value is ``False``, there was no
      object to remove.

.. py:class:: DeferredFromAttrs

   .. py:method:: wait()

      The return value is a boolean indicating success.  If the value is
      ``True``, the object exists, and all values were atomically changed.
      If the value is ``False``, the object was not found.

.. py:class:: Search(client, space, predicate)

   .. py:method:: __next__()

      Return the next object or exception resulting from the search.  Objects
      are Python dictionaries mapping attributes to their values.
