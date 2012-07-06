.. _datamodel:

Data Model
==========

HyperDex's data model is powerful and straightforward making it very easy to
create robust and clear-cut applications.  The simplicity of the datamodel is
paramount as it simultaneously enables HyperDex to efficiently store and
retrieve your data while offering you a transparent conceptual model for
HyperDex's behavior.

This chapter is dedicated to describing how HyperDex interprets and handles your
data.  At its core, HyperDex is a key-value store.  The first half of this
chapter explores what this means for you and your data.  The remainder explores
how HyperDex may store multiple distinct types of data in a single cluster.

Keys and Values and Objects, Oh My!
-----------------------------------

The basic unit of storage in HyperDex is an *object*.  Each object is a set of
*attributes* and corresponding *attribute values*.  For example, a phone book
application may store the object
``{'name': 'John Smith': 'phone_number': '607-555-1024'}`` [#dict]_
"1-607-555-1024".  In this case, ``'name'`` and ``'phone_number'`` are
attributes while ``'John Smith'`` and ``'607-555-1024'`` are the attribute
values of the object.  The distinction becomes more clear when considering
multiple objects of similar type.  All objects from the phone book will share
the same set of attributes, but the attribute values of each object will be as
varied as the population.

To facilitate object storage and retrieval, each object is associated with an
application-provided *key*.  Each object has exactly one key which, in turn,
refers directly to that object.  This provides a convenient way for an
application to identify objects located in HyperDex.

HyperDex is a key value store.  The key is, well, the key and the value is the
object that is stored.  Like a typical key-value store, HyperDex allows fast and
efficient key-based operation; however, HyperDex goes far beyond the simple
interface of most key-value stores and enables the user to select a different
datatype for each object attribute.

Rich Datatypes for Rich Applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

HyperDex offers a wide range of useful datatypes.  There are two classes of
datatypes in HyperDex:  primitive and complex datatypes.  Primitive datatypes
are self-contained and pieces of plain data.  Complex datatypes are
compositions of primitive datatypes.

Primitive datatypes:
 * ``string``:  A bytestring.  May contain any byte 0-255.
 * ``int64``:  A signed 64-bit number.
   These numbers are stored in Little Endian format.
 * ``float``:  A double-precision IEEE 754 floating point number.
   These numbers are stored in Little Endian format.

Complex datatypes:
 * ``list(T)``:  A list with elements of type ``T``, where ``T`` is any
   primitive type.
 * ``set(T)``:  A set of elements of type ``T``, where ``T`` is any primitive
   type.
 * ``map(K, V)``:  An associative map from elements of type ``K`` to type ``V``
   where ``K`` and ``V`` are primitive types.

Every attribute of an object has a distinct type, making it easy to build and
maintain complex objects within HyperDex.

Organizing Data in Spaces
-------------------------

HyperDex enables distinct groups of objects through the :term:`space`
abstraction.  A space holds zero or more objects and specifies all attributes
and their respective types for the objects within the space.  All objects within
the space conform to the space's schema.  This representation conveniently
resembles the table abstraction available in existing databases.

Although the external representation of a :term:`space` closely resembles the
table abstraction, HyperDex's internal representation is fundamentally
different.  HyperDex uses :term:`hyperspace hashing` to determine how to
distribute objects onto nodes in a cluster.  Specifically, it maps objects with
multiple attributes into points in a multidimensional :term:`hyperspace`.  While
the details of hyperspace hashing are best left to the `HyperDex white paper
<http://hyperdex.org/papers/>`_, there are several distinct user-visible
features of hyperspace hashing:

 - By storing data in a hyperspace, HyperDex is able to expand the typical
   key-value API with a unique ``search`` operation.
 - Searches in HyperDex are fast.  In the best case, each search contacts just
   one server, eliminating problematic fan-out patterns that reduce throughput.
 - Each server reapplies hyperspace hashing to group similar data, providing
   results quickly.
 - Search queries benefit from specificity in the search predicate.  Adding
   filters to a search can only improve the speed with which items are retrieved
   from disk.

This last point provides large efficiency gains.  Specifying all attributes of
an object will contact exactly one server which, in turn, looks in exactly one
location on-disk for the matching object.  Often, however, it is extremely
desirable to specify a subset of the (possibly many) attributes of an object.
HyperDex ensures this case is efficient by allowing the user to influence
HyperDex's data layout.

Subspaces for Efficient Search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

HyperDex provides users with a simple and direct way to improve search
efficiency by telling HyperDex which attributes are likely to be queried
together.  To do so, the user creates a :term:`subspace` underneath the space
which describes which attributes of the object may be used to search in the
subspace.  This has two significant user-visible effects:

 - By defining a subspace, the user can tell HyperDex that the attributes are
   likely to be queried together.  A search that specifies all attributes of a
   subspace will be just as efficient as a search which specifies all
   attributes.
 - Each subspace creates an additional copy of the objects stored within.
   HyperDex automatically maintains these copies.

The most general guideline when designing a subspace is to pick attributes such
that the majority of attributes will be provided as part of the predicate for
any given search.

.. [#dict] This syntax intentionally resembles Python dictionaries and JSON
   objects.
