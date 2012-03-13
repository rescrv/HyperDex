:orphan:

hyperdex-daemon manual page
===========================

Synopsis
--------

**hyperdex-daemon** [*options*]


Description
-----------

:program:`hyperdex-daemon` is a high performance data storage daemon.  Each
:program:`hyperdex-daemon` is designed to be part of a larger cluster, and
holds only a portion of the data stored in the cluster.

The daemon is designed to be as close to zero-conf as possible.  The options
shown below are the only parameters that a user must specify.


Options
-------

.. option:: -\?, --help

   Show a help message

.. option:: -d, --daemon

   Run HyperDex in the background

.. option:: -f, --foreground

   Run HyperDex in the foreground

.. option:: -D, --data=D

   Directory to store all data and log files.  It must exist.  If no directory
   is specified, the current directory is used.

.. option:: -h, --host=IP

   IP address to use when connecting to the coordinator

.. option:: -p, --port=P

   Port to use when connecting to the coordinator

.. option:: -t, --threads=N

   Number of threads to create which will handle client requests.  This should
   be equal to the number of cores for workloads which may be cached by main
   memory.

.. option:: -b, --bind-to=IP

   Local IP address on which to handle network requests.  This must be
   addressable and unique among all nodes in the HyperDex cluster.

.. option:: -i, --incoming-port=P

   Port to listen on for incoming connections.  If none is specified, a port is
   selected at random.  It is best to let HyperDex select a random port.

.. option:: -o, --outgoing-port=P

   Port to use for all outgoing connections.  If none is specified, a port is
   selected at random.  It is best to let HyperDex select a random port.


See also
--------

* :manpage:`hyperdex-binary-test(1)`
* :manpage:`hyperdex-coordinator(1)`
* :manpage:`hyperdex-coordinator-control(1)`
* :manpage:`hyperdex-daemon(1)`
* :manpage:`hyperdex-replication-stress-test(1)`
* :manpage:`hyperdex-simple-consistency-stress-test(1)`
