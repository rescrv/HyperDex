:orphan:

hyperdex-simple-consistency-stress-test manual page
===================================================

Synopsis
--------

**hyperdex-simple-consistency-stress-test** [*options*]


Description
-----------

:program:`hyperdex-simple-consistency-stress-test` puts the fault tolerance code
its paces.  This code is designed to operate on the following space::

    space consistency
    dimensions n, repetition
    key n auto 0 2

The stress tester spawns a single writer thread which writes to keys in the
range [0, window-size) in order with strictly increasing values for
``repetition``.  Multiple reader threads read in reverse order, looking out for
a case in which the ``repetition`` value of successive keys goes backwards.

It is expected that failures will be generated externally (e.g. by forced
process kills or dropped network connections) while running this test.


Options
-------

.. option:: -\?, --help

   Show a help message.

.. option:: -w, --window-size=keys

   The number of sequential keys which will be used for the test.  The smaller
   this number, the more likely it is to trigger an error condition.  Use the
   largest value possible when hunting a bug.

.. option:: -r, --repetitions=number

   The number of repetitions to run before exiting.  Each key in [0,
   window-size) will be written this many times.  A higher value runs for
   longer.

.. option:: -t, --threads=number

   The number of reader threads which will check for inconsistencies.

.. option:: -s, --space=space

   The name of the space to operate on.  By default "consistency" is used.

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
