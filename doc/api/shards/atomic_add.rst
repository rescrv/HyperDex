An ``atomic_add`` request adds the amount specified to the value of each
attribute specified by the request.  The attributes will be modified atomically
without interference from other operations.

Consistency:
   ``atomic_add`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomic_add``
   request completes will see the results of that ``atomic_add``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomic_add`` is the same as a ``put``.
