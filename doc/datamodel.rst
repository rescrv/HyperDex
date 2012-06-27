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

Datatypes
~~~~~~~~~

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
 * ``list(T)``:  A list with elements of type ``T``.
 * ``set(T)``:  A set of elements of type ``T``.
 * ``map(K, V)``:  An associative map from elements of type ``K`` to type ``V``.

Spaces
------

HyperDex enables distinct groups of objects through the *space* abstraction.  A
space holds zero or more objects and specifies all attributes and their
respective types for the objects within the space.  All objects within the space
conform to the space's schema.  This representation conveniently resembles the
table abstraction available in existing databases.

XXX

Subspaces
~~~~~~~~~

XXX

.. [#dict] This syntax intentionally resembles Python dictionaries and JSON
   objects.
