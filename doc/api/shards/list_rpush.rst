A ``list_rpush`` request pushes the specified object to the tail of the list of
each attribute specified by the request.  The attributes will be modified
atomically without interference from other operations.

Consistency:
   ``list_rpush`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``list_rpush``
   request completes will see the results of that ``list_rpush``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``list_rpush`` is the same as a ``put``.
