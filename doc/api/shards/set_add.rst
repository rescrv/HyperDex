A ``set_add`` request adds a single element to a set or does nothing if the
element is already in the set.  The insertion will be atomic without
interference from other operations.

Consistency:
   ``set_add`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``set_add``
   request completes will see the results of that ``set_add``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``set_add`` is the same as a ``put``.
