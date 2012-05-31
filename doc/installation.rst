Installing HyperDex
===================

For the purposes of this document, we will need a working copy of
HyperDex. This document makes use of features available
in HyperDex 0.4 and later. There are two ways of installing the
most recent version of the system: from precompiled binaries, or
from source.

Installing Precompiled Binaries
-------------------------------

The easiest way to install the system is through the precompiled packages. The
HyperDex distribution currently supports Debian, Ubuntu and Fedora. The HyperDex
download page has instructions on how to add the HyperDex repository so a simple
install operation will get the most recent binaries on your system.

The precompiled binaries have the necessary dependencies built into them so
installing them should pull in all other required components.

Installing From Source
----------------------

Building from source is recommended when prebuilt packages are
unsuitable or unavailable for your environment. In addition to the
source tarballs provided below, you'll need to install Google's
CityHash (we test against 1.0.x), Google's glog (we test against
0.3.x) and libpopt (we use 1.16). Additionally, you'll need libpo6 (version
0.2.3 or newer), libe (version 0.2.7 or newer), busybee (version 0.1.0 or newer)
and the HyperDex source distribution. The latter four can be found on the
HyperDex download page.

HyperDex is extremely easy to build from source. Once you've installed all of
the prerequisites, you can install HyperDex with:

.. sourcecode:: console

   $ ./configure --enable-python-bindings --prefix=/path/to/install
   $ make
   # make install

Summary
-------

Once you have HyperDex installed, you should be able to start the
coordinator and daemon processes as follows.

.. sourcecode:: console

   $ hyperdex-coordinator -?
   $ hyperdex-daemon -?

Once you get to this stage, HyperDex is ready to go. We can set up a
data store and examine how to place values in it, which is the topic
of the next chapter.
