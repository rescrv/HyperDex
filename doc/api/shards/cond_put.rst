A ``cond_put`` request creates or updates the attributes of an object stored in a
specific space under a specific key if and only if certain attributes match the
specified condition..  Only those attributes specified will be affected by the
``cond_put`` operation.  If the object does not exist, the operation will fail.
All unspecified attributes will be left to their current values.

Consistency:
   ``cond_put`` requests to the same key will be observed in the same order by
   all clients.  All ``get`` requests which execute after a given ``cond_put``
   request completes will see the result of that ``cond_put``, or of a later
   operation which superseded it.

Efficiency:
   The dominating cost of a ``cond_put`` is the same as a ``put`` when the
   comparison succeeds.  If the comparison fails, the cost is the same as a
   ``get``.
