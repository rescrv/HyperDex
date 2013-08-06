A ``get`` operation will return the latest object's attributes on succes, or
will return not found.  The operation is guaranteed to be linearizable.  The
predominant cost of a ``get`` is one network round trip, and one (possibly
cached) disk access.
