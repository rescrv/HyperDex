An ``atomicdec`` request decrements the value of every attribute specified by
the request.  The attributes will be decremented atomically without interference
from other operations.

Consistency:
   ``atomicdec`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``atomicdec``
   request completes will see the results of that ``atomicdec``, or of a later
   operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``atomicdec`` is the same as a ``put``.
