An ``atomic_and`` request stores the bitwise-and of the amount specified with
the value of each attribute specified by the request.  The attributes will be
modified atomically without interference from other operations.

Consistency:
   ``atomic_and`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomic_and``
   request completes will see the results of that ``atomic_and``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomic_and`` is the same as a ``put``.
