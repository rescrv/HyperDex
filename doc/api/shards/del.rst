A ``del`` request removes an object identified by a key from the specified
space.  If the object does not exist, the operation will do nothing.

Consistency:
   ``del`` requests to the same key will be observed in the same order by all
   clients.  All ``get`` requests which execute after a given ``del`` request
   completes will see the result of that ``del``, or of a later operation which
   superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of a ``del`` is one network round trip per replica for
   cached objects, and one disk read per replica for uncached objects.
