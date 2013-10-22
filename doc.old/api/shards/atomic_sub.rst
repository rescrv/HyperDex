An ``atomic_sub`` request subtracts the amount specified from the value of each
attribute specified by the request.  The attributes will be modified atomically
without interference from other operations.

Consistency:
   ``atomic_sub`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomic_sub``
   request completes will see the results of that ``atomic_sub``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomic_sub`` is the same as a ``put``.
