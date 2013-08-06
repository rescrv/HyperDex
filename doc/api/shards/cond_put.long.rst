A successfull ``cond_put`` will overwrite all specified attributes with the new
values specified by ``attrs``.  Any attributes not specified by the ``cond_put``
call will be unchanged.

The operation will have an effect if and only if all conditions specified by
``checks`` are true for the latest version of the object.  This check is atomic
with the write.  If the object does not exist, the checks will fail.

``cond_put`` operations are guaranteed to be linearizable.  The operations are totally
ordered by the servers responsible for storing the object.

The cost of a ``cond_put`` is proportional to length of the value-dependent chain formed
for the object, and incurs a disk write on each node in the chain.
