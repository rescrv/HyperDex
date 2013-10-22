A ``set_intersect`` performs set intersection, and stores the resulting set in the object.
The intersection will be atomic without interference from other operations.

Consistency:
   ``set_intersect`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given
   ``set_intersect``
   request completes will see the results of that ``set_intersect``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``set_intersect`` is the same as a ``put``.
