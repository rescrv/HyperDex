:orphan:

hyperdex-replication-stress-test manual page
============================================

Synopsis
--------

**hyperdex-replication-stress-test** [*options*]


Description
-----------

:program:`hyperdex-replication-stress-test` puts the replication code through
its paces.  There are certain potential race conditions that this code is
designed to expose.  To execute the test, create a space with the following
description::

    space replication
    dimensions A, B, C
    key A auto 4 2
    subspace B auto 4 2
    subspace C auto 4 2

The code will execute a number of operations in quick succession in a manner
that bounces objects around the subspaces for ``B`` and ``C``.  This test
relies upon every host in the cluster being deployed with a single network
thread (``--threads=1``).


Options
-------

.. option:: -\?, --help

   Show a help message.

.. option:: -P, --prefix=number

   The power of two used to indicate the size of the space.

.. option:: -s, --space=space

   The name of the space to operate on.  By default "replication" is used.

.. option:: -h, --host=IP

   IP address to use when connecting to the coordinator

.. option:: -p, --port=P

   Port to use when connecting to the coordinator


See also
--------

* :manpage:`hyperdex-binary-test(1)`
* :manpage:`hyperdex-coordinator(1)`
* :manpage:`hyperdex-coordinator-control(1)`
* :manpage:`hyperdex-daemon(1)`
* :manpage:`hyperdex-replication-stress-test(1)`
* :manpage:`hyperdex-simple-consistency-stress-test(1)`
