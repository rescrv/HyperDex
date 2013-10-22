A ``group_del`` request removes objects which match the specified predicate.

Consistency:
   ``group_del`` will remove objects that exist prior to the start of the
   ``group_del`` operation.

Efficiency:
   The dominating cost of a search is one network round trip per object.
   Servers issue ``del`` operations directly, but the client waits for all
   servers to finish before returning "SUCCESS".
