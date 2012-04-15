A ``set_union`` performs set union, and stores the resulting set in the object.
The union will be atomic without interference from other operations.

Consistency:
   ``set_union`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``set_union``
   request completes will see the results of that ``set_union``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``set_union`` is the same as a ``put``.
