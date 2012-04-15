An ``atomic_mod`` request computes the value of each attribute specified by the
request modulo the amount specified.  The attributes will be modified atomically
without interference from other operations.

Consistency:
   ``atomic_mod`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomic_mod``
   request completes will see the results of that ``atomic_mod``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomic_mod`` is the same as a ``put``.
