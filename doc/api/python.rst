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

   .. py:method:: condput(space, key, condition, attrs)

      .. include:: shards/condput.rst

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

   .. py:method:: atomicinc(space, key, attrs)

      .. include:: shards/atomicinc.rst

      The return value is a boolean indicating success.  If the value is
      ``True``, the object exists, and all values were atomically incremented.
      If the value is ``False``, the object was not found.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to the amount by which the
         corresponding values will be incremented.  The specified attributes
         must be numeric in nature.

   .. py:method:: atomicdec(space, key, attrs)

      .. include:: shards/atomicdec.rst

      The return value is a boolean indicating success.  If the value is
      ``True``, the object exists, and all values were atomically decremented.
      If the value is ``False``, the object was not found.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to the amount by which the
         corresponding values will be decremented.  The specified attributes
         must be numeric in nature.

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

   .. py:method:: async_condput(space, key, condition, value)

      .. include:: shards/condput.rst

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

   .. py:method:: async_atomicinc(space, key, value)

      .. include:: shards/atomicinc.rst

      The returned object will be a :py:class:`DeferredAtomicIncDec` instance
      which tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to the amount by which the
         corresponding values will be incremented.  The specified attributes
         must be numeric in nature.

   .. py:method:: async_atomicdec(space, key, value)

      .. include:: shards/atomicdec.rst

      The returned object will be a :py:class:`DeferredAtomicIncDec` instance
      which tracks the request.

      space:
         A string naming the space in which the object will be inserted.

      key:
         The key of the object.  Keys may be either byte strings or integers.

      attrs:
         A dictionary mapping attribute names to the amount by which the
         corresponding values will be decremented.  The specified attributes
         must be numeric in nature.

   .. py:method:: loop()


.. py:class:: HyperClientException(status, attr)

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

.. py:class:: DeferredAtomicIncDec

   .. py:method:: wait()

      The return value is a boolean indicating success.  If the value is
      ``True``, the object exists, and all values were atomically changed.
      If the value is ``False``, the object was not found.

.. py:class:: Search(client, space, predicate)

   .. py:method:: __next__()

      Return the next object or exception resulting from the search.  Objects
      are Python dictionaries mapping attributes to their values.
