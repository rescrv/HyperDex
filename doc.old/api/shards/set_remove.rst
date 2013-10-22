A ``set_remove`` request removes a single element to a set, or does nothing if
the element is not in the set.  The removal will be atomic without
interference from other operations.

Consistency:
   ``set_remove`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``set_remove``
   request completes will see the results of that ``set_remove``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``set_remove`` is the same as a ``put``.
