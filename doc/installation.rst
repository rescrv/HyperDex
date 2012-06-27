.. _installation:

Installing HyperDex
===================

HyperDex provides two modes of installation for users.  The easiest and most
convenient way to install HyperDex is to use precompiled binary packages.
Packages are available for a several platforms and more platforms will be
supported as resources permit.  For users who need more control over their
installation, HyperDex provides the option to compile from source tarballs.

Installing Binary Packages
--------------------------

The HyperDex team maintains repositories for Debian_, Ubuntu_, and Fedora_ so
that the latest version is always conveniently available.  Package are named
consistently across platforms.  This section first explores the packages of
interest and then provides platform-specific instructions for how to access the
binary packages.

On all architectures, the ``hyperdex`` meta-package pulls in all HyperDex
packages.  For most users installing the ``hyperdex`` package is sufficient.
For completeness, here is a list of all packages:

.. note::

   Don't worry if the following descriptions describe pieces of HyperDex that
   you are unfamiliar with.  In the following chapters, we'll be describing each
   component in-detail.  It is safe to install the ``hyperdex`` package and skip
   this package listing.

``hyperdex-daemon``:
   This package contains the HyperDex daemon that runs on each storage node.  It
   required on every storage node.

``libhyperclient``:
   This package contains the client library for C/C++ bindings.  It is
   required on all platforms which will access HyperDex.

   On Debian and Ubuntu systems, this will have a small number appended to the
   package name indicating the version of the package contained within.

``python-hypercoordinator``:
   This package provides the coordinator for a HyperDex cluster.  This package
   is required only on systems which will serve as the coordinator for the
   cluster.

``python-hyperclient``:
   This provides the python module :py:mod:`hyperclient`.  This package is only
   required for systems that need to interact with HyperDex from Python.

Most packages are coplemented by development and debug packages.  In the
development package, there are header files and static libraries.  The debug
packages provide symbols which will aid in providing tracebacks to the HyperDex
developers.  Please consult your package manager to find these packages.

Debian
~~~~~~

To access the Debian repository, add the following to ``/etc/apt/sources.list``::

   deb http://debian.hyperdex.org squeeze main

Subsequent invocations of the package manager may complain about the absence of
the relevant package signing key.  You can download the `Debian public key`_ and
add it with:

.. sourcecode:: console

   apt-key add hyperdex.gpg.key

.. _Debian public key: http://debian.hyperdex.org/hyperdex.gpg.key

The following code may be copied and pasted to quickly setup the Debian
repository::

   wget -O - http://debian.hyperdex.org/hyperdex.gpg.key \
   | apt-key add -
   wget -O /etc/apt/sources.list.d/hyperdex.list \
       http://debian.hyperdex.org/hyperdex.list
   aptitude update

To install HyperDex, issue the following command as root:

.. sourcecode:: console

   aptitude install hyperdex

Ubuntu
~~~~~~

To access the Ubuntu repository, add the following to
``/etc/apt/sources.list``::

   deb [arch=amd64] http://ubuntu.hyperdex.org oneiric main

Subsequent invocations of the package manager may complain about the absence of
the relevant package signing key.  You can download the `Ubuntu public key`_ and
add it with:

.. sourcecode:: console

   apt-key add hyperdex.gpg.key

.. _Ubuntu public key: http://ubuntu.hyperdex.org/hyperdex.gpg.key

The following code may be copied and pasted to quickly setup the Ubuntu
repository::

   wget -O - http://ubuntu.hyperdex.org/hyperdex.gpg.key \
   | apt-key add -
   wget -O /etc/apt/sources.list.d/hyperdex.list \
       http://ubuntu.hyperdex.org/hyperdex.list
   apt-get update

To install HyperDex, issue the following command as root:

.. sourcecode:: console

   apt-get install hyperdex

Fedora
~~~~~~

To access the Fedora repository, add the following to ``/etc/yum.conf``::

   [hyperdex]
   name=hyperdex
   baseurl=http://fedora.hyperdex.org/
   enabled=1
   gpgcheck=0

To install HyperDex, issue the following command as root:

.. sourcecode:: console

   yum install hyperdex

.. _Debian: http://www.debian.org
.. _Ubuntu: http://www.ubuntu.com
.. _Fedora: http://fedoraproject.org

Installing Source Tarballs
--------------------------

Installing HyperDex from source is straightforward and should work on most any
recent Linux distribution.  We'll first list the prerequisites to installing
HyperDex.  Then, we'll describe how to configure HyperDex.  Finally, we'll
describe the installation step.

Prerequisites
~~~~~~~~~~~~~

HyperDex has a minimal number of prerequisites for installation.  Although we
list all prerequisites in this section for completeness, the HyperDex
configuration step will automatically warn about any missing dependencies.

Required Dependencies:

 * `Google CityHash`_:  Used for hashing strings.  Requires version 1.0.x
 * `Google Glog`_:  Used for logging.  Requires version 0.3.x.
 * libpopt_:  Used for argument parsing.  The developers use 1.16 but any
   recent version should do.
 * libpo6_:  Used for general POSIX support.  Requires the latest version.
   This package is maintained by the HyperDex developers.
 * libe_:  Used for general C++ utilities.  Requires the latest version.
   This package is maintained by the HyperDex developers.
 * BusyBee_:  Used for server-server communication.  Requires the latest
   version.  This package is maintained by the HyperDex developers.

Dependencies for Tests:

 * `Google Gtest`_: Used for test suites.  Requires version 1.5.x.

Dependencies for Python Bindings:

 * Python_: Version 2.6 or 2.7 with the development headers installed.

Dependencies for Java Bindings:

 * Java_:  We test against OpenJDK 6.  Your system must include ``javac``,
   ``jar``, and the JNI development headers.
 * SWIG_:  Used to generate part of the bindings.  We test SWIG 2.0.

Dependencies for Yahoo! Cloud Serving Benchmark (YCSB):

 * YCSB_:  The YCSB distribution is a moving target.  We generally build against
   the latest Git release.

.. _Google CityHash: http://code.google.com/p/cityhash/
.. _Google Glog: http://code.google.com/p/google-glog/
.. _libpopt: http://rpm5.org/
.. _Google Gtest: http://code.google.com/p/googletest/
.. _Python: http://python.org/
.. _Java: http://openjdk.java.net/
.. _SWIG: http://www.swig.org/
.. _YCSB: https://github.com/brianfrankcooper/YCSB/wiki
.. _libpo6: http://hyperdex.org
.. _libe: http://hyperdex.org
.. _BusyBee: http://hyperdex.org

Configuring
~~~~~~~~~~~

HyperDex uses the Autotools to make configuration and installation as
straightforward as possible.  After extracting the HyperDex tarball, you'll need
to configure HyperDex.  The simplest configuration installs HyperDex in its
default location (``/usr/local``) using the first C++ compiler found on the
system.  The configuration is performed in the directory extracted from the
tarball and looks like:

.. sourcecode:: console

   ./configure

This basic configuration will configure the HyperDex daemon and native client
library components to be built; however it omits several useful options for
configuring HyperDex.  The rest of this section will highlight common
ways to configure HyperDex.  Unless otherwise noted, all options should work
well together.

Enabling Java Bindings:
   HyperDex does not build Java bindings by default.  To enable the Java
   bindings, you must pass ``--enable-java-bindings`` to ``./configure`` like
   so:

   .. sourcecode:: console

      ./configure --enable-java-bindings

   If any of the prerequisites_ are missing ``./configure`` will fail.

Enabling Python Bindings:
   HyperDex does not build Python bindings by default.  To enable the Python
   bindings, you must pass ``--enable-python-bindings`` to ``./configure`` like
   so:

   .. sourcecode:: console

      ./configure --enable-python-bindings

   If Python or its headers cannot be found, ``./configure`` will fail.

Enable the Yahoo! Cloud Serving Benchmark:
   HyperDex provides all the source code necessary to build a HyperDex driver
   for the YCSB benchmark.  If Java bindings are enabled, then YCSB can be built
   with ``--enable-ycsb-driver``.

   .. sourcecode:: console

      ./configure --enable-ycsb-driver

   Note that YCSB must be in your Java CLASSPATH.  Configure will not detect
   YCSB by itself.

Changing the Installation Directory:
   By default HyperDex installs files in ``/usr/local``.  If you'd like to
   install HyperDex elsewhere, you can specify the installation prefix at
   configure time.  For example, to install HyperDex in ``/opt/hyperdex``:

   .. sourcecode:: console

      ./configure --prefix=/opt/hyperdex

   Check the ``--help`` option to configure for more ways to tune where HyperDex
   places files.

Installing
~~~~~~~~~~

Once configured, HyperDex is simple to build and install.  Keep in mind that the
following commands may fail if the installation directory is not writable to the
current user.

.. sourcecode:: console

   make
   make install

Verifying Installation
----------------------

Once you have HyperDex installed, you should be able to view the coordinator
and daemon programs' built-in help with the following:

.. sourcecode:: console

   hyperdex-coordinator --help
   hyperdex-daemon --help

If the above commands provide helpful output, then it is very likely that
HyperDex is installed correctly and ready for use.
