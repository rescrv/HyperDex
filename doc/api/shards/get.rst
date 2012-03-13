A ``get`` request retrieves an object from a specific space using the object's
key.  If the object exists in the space at the time of the request, it will be
returned; otherwise, the API will indicate that the object was not found.

Consistency:
   A ``get`` request will always see the result of all complete ``put``
   requests.

   .. include:: shards/linearizable.rst

Efficiency:
   The dominating cost of a ``get`` is one network round trip for cached
   objects, and one disk read for uncached objects.
