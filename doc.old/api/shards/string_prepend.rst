A ``string_prepend`` request prepends the specified string to the value of each
attribute specified by the request.  The attributes will be modified atomically
without interference from other operations.

Consistency:
   ``string_prepend`` requests to the same key will be observed in the same
   order by all clients.  All ``get`` requests which execute after a given
   ``string_prepend`` request completes will see the results of that
   ``string_prepend``, or of a later operation which superseded it.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of an ``string_prepend`` is the same as a ``put``.
