A ``count`` counts the  number of objects which match the specified predicate.
These objects may reside on multiple nodes, and the client transparently handles
aggregating counts across nodes.  If the count fails with an error, the count
specifies a lower-bound on the number of objects.

Consistency:
   ``count`` results will see the result of every operation which completes
   prior to the search, unless there is a concurrent operation which supersedes
   the prior operation.

Efficiency:
   The dominating cost of a count is one network round trip per object.
