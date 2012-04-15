The ``loop`` call is the core event loop that receives and processes server
responses.  In the normal case, ``loop`` will return exactly one completed
operation, or in the case of search, one item matching the search result.

It is imperative that the application periodically call ``loop`` in order to
prevent unbounded resource consumption.  Only in the ``loop`` function does the
client library perform I/O which will clear the ``recv`` socket buffers.
