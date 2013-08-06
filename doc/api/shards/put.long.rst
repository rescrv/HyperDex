A successfull ``put`` will overwrite all specified attributes with the new
values specified by ``attrs``.  Any attributes not specified by the ``put`` call
will be unchanged -- when the object exists prior to the ``put``, the previous
value of the attribute is maintained; when the object does not exist, the values
will be initialized to their defaults.

``put`` operations are guaranteed to be linearizable.  The operations are
totally ordered by the servers responsible for storing the object.

The cost of a ``put`` is proportional to length of the value-dependent chain formed
for the object, and incurs a disk write on each node in the chain.
