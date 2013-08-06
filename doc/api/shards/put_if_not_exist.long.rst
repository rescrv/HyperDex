A successfull ``put_if_not_exist`` will overwrite all specified attributes with
the new values specified by ``attrs``.  Any attributes not specified by the
``put_if_not_exist`` call will be initialized to their defaults.

The operation will have an effect if and only if the object does not exist.
This check is atomic with the write, and it is guaranteed to never overwrite an
existing object.

``put_if_not_exist`` operations are guaranteed to be linearizable.  The
operations are totally ordered by the servers responsible for storing the
object.

The cost of a ``put_if_not_exist`` is proportional to length of the
value-dependent chain formed for the object, and incurs a disk write on each
node in the chain.
