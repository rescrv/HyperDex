An ``atomic_div`` request divides the value of each attribute specified by the
request by the amount specified.  The attributes will be modified atomically
without interference from other operations.

Consistency:
   ``atomic_div`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomic_div``
   request completes will see the results of that ``atomic_div``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomic_div`` is the same as a ``put``.
