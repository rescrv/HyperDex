An ``atomicinc`` request increments the value of every attribute specified by
the request.  The attributes will be incremented atomically without interference
from other operations.

Consistency:
   ``atomicinc`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomicinc``
   request completes will see the results of that ``atomicinc``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomicinc`` is the same as a ``put``.
