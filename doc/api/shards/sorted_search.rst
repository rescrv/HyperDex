A ``sorted_search`` request retrieves all objects which match the specified
predicate.  These objects may reside on multiple nodes, and the client
transparently handles retrieving them from each node.  Multiple errors may be
returned from a single search; however, the client library will continue to make
progress until all servers finish the search or encounter an error.  If
sorted_search completes without error, the results are guaranteed to be the
first or last ``limit`` results sorted according to ``attr``.

Consistency:
   ``sorted_search`` results will see the result of every operation which
   completes prior to the search, unless there is a concurrent operation which
   supersedes the prior operation.

Efficiency:
   The dominating cost of a search is one network round trip per object.
   Multiple objects will be retrieved in parallel, so searches should be more
   efficient than simple object retrieval.
