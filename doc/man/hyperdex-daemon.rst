:orphan:

hyperdex-daemon manual page
===========================

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

.. option:: -c, --coordinator=IP

   IP address to use when connecting to the coordinator

.. option:: -P, --coordinator-port=P

   Port to use when connecting to the coordinator

.. option:: -t, --threads=N

   Number of threads to create which will handle client requests.  This should
   be equal to the number of cores for workloads which may be cached by main
   memory.

.. option:: -l, --listen=IP

   Local IP address on which to handle network requests.  This address must be
   reachable from all nodes in the HyperDex cluster.

.. option:: -p, --listen-port=P

   Port to listen on for incoming connections.  Default: 2012.
