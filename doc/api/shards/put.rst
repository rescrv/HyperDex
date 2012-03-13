A ``put`` request creates or updates the attributes of an object stored in a
specific space under a specific key.  Only those attributes specified will be
affected by the ``put`` operation.  If the object does not exist, it will be
created, and all specified attributes will be set to the values provided.  All
other attributes will be initialized to their default state.

Consistency:
   ``put`` requests to the same key will be observed in the same order by all
   clients.  All ``get`` requests which execute after a given ``put`` request
   completes will see the result of that ``put``, or of a later operation which
   superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of a ``put`` is one network round trip per replica for
   cached objects, and one disk read per replica for uncached objects.
