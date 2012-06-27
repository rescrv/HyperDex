Introduction
============

Why HyperDex?
-------------

HyperDex is a distributed, searchable key-value store.  HyperDex embodies a
unique set of features not found together in other NoSQL systems or RDMBSs:

 * **Fast**:  HyperDex has lower latency, higher throughput, and lower variance
   than other key-value stores.

 * **Scalable**:  HyperDex scales as more machines are added to the system.

 * **Consistent**:  HyperDex guarantees linearizability for key-based
   operations.  Thus, a ``GET`` always returns the latest value inserted into
   the system.  Not just "eventually," but immediately and always.

 * **Fault Tolerant**:  HyperDex automatically replicates data on multiple
   machines so that concurrent failures, up to an application-determined limit,
   will not cause data loss.

 * **Searchable**:  HyperDex enables lookups of secondary data attributes. Such
   searches are implemented efficiently and contact a small number of servers.

 * **Easy-to-Use**:  HyperDex provides an easy-to-use API across a variety of
   scripting and native languages.

 * **Maintainable**:  HyperDex makes it easy to setup and maintain a cluster of
   servers.

HyperDex is designed from the ground up to make it easy to develop new
applications.  The user-visible API provides clean semantics with well defined
behavior under all circumstances.  Further, HyperDex facilitates
high-performance applications by ensuring that every operation is fast and has
quantifiable runtime.

About this Book
---------------

This book is the definitive reference manual for HyperDex users and developers.
The first half of the book is devoted to users of HyperDex.
:ref:`Chapter 2 <installation>` provides instruction for installing the latest
HyperDex release from either precompiled packages or source tarballs [#devinst]_.
:ref:`Chapter 3 <datamodel>` describes the HyperDex data model and basic
terminology used throughout the rest of the book.
:ref:`Chapter 4 <quickstart>` is a short tutorial that demonstrates how to setup
and interact with a HyperDex cluster.
:ref:`Chapter 5 <keyoperations>` and :ref:`Chapter 6 <searchoperations>`
describe in detail the plethora of operations provided as part of the HyperDex
API.
In :ref:`Chapter 7 <administration>`, we cover basic administration of a
HyperDex cluster.
The entire API is described in :ref:`Chapter 8 <api>`.
For completeness, man pages are included in :ref:`Chapter 9 <man>`.
:ref:`Chapter 10 <dev>` is intended for developers of HyperDex itself.

The HyperDex developers are constantly improving both HyperDex and this
reference manual.  If you would like to provide feedback on this manual, please
use the contact information provided in the next section to help the HyperDex
team make this manual even better.

Online Help and Support
-----------------------

If you run into trouble while working with HyperDex, don't panic!  The HyperDex
team and community maintain many free resources online designed to help users
who are having trouble with HyperDex and this reference manual.

 * `HyperDex Discuss Mailing List`_:  This open mailing list is most users'
   first resource solving problems.  Anyone may email the list seeking help and
   messages are archived for the benefit of all.  Please follow proper `mailing
   list etiquette`_ to make it easy for us to help you.  By doing so, you'll not
   only make it easy for us to answer your question and provide a quick answer,
   but you'll also leave a useful message in the archive to help future users.

 * `HyperDex IRC Channel`_:  The **#hyperdex** IRC channel on
   **irc.freenode.net** is a great place to ask questions and interact with
   HyperDex's developers in real-time.  We thank the Freenode_ project for
   hosting the resource for us.

 * `HyperDex Bug Tracker`_:  If you've encountered an error in the code that you
   think may be a bug, please check the bug tracker to see if other users have
   reported the same problem.  Consider reporting the problem yourself if it
   looks like the problem has not yet been reported.

Please keep in mind that many of the people you'll meet through these resources
are volunteers working on HyperDex in their free time.  If you have more-urgent
needs, you can contact the team of core HyperDex developers directly at
*hyperdex-team* **at** *systems* *dot* *cs* *dot* *cornell* *dot* *edu*

.. _HyperDex Discuss Mailing List: https://groups.google.com/group/hyperdex-discuss
.. _mailing list etiquette: http://www.freebsd.org/doc/en_US.ISO8859-1/articles/mailing-list-faq/etiquette.html
.. _HyperDex IRC Channel: http://webchat.freenode.net/?channels=hyperdex&uio=d4
.. _HyperDex Bug Tracker: https://github.com/rescrv/HyperDex/issues
.. _Freenode: http://freenode.net/

.. [#devinst] Instructions for building HyperDex from the Git repo are provided
   in the developer's section.
