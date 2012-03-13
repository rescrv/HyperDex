Concepts
========

HyperDex is a distributed, searchable key-value store. Unlike other key-value
stores such as BigTable and Cassandra, HyperDex provides a unique search
primitive which enables searches over stored values. By design, HyperDex
retains the performance of traditional key-value stores while enabling support
for the search operation.  This is possible through the use of Hyperspace
Hashing, a new technique that maps objects with multiple attributes into points
in a multidimensional hyperspace. Search queries on secondary object attributes
can therefore be mapped to small, hyperspace regions representing the set of
feasible locations for the matching objects. This geometric mapping enables
efficient searches that do not require enumerating across every object in the
system. The following sections detail how HyperDex places servers and objects
in the Hyperspace, and addresses classic problems with using high dimensional
data-structures.

Node and Object Placement
-------------------------

HyperDex strategically places objects on servers so that both search and
key-based operations contact a small subset of all servers in the system.
Whereas typical key-value stores map objects to nodes using just the key,
HyperDex takes into account all attributes of an object when mapping it to
servers.

As was mentioned in the previous section, HyperDex uses Hyperspace Hashing to
map objects to points in a multidimensional space.  Hyperspace hashing creates
a multidimensional euclidean space for each table, where a table holds objects
with the same attribute types, and each attribute type corresponds to a
dimension in the euclidean space. HyperDex determines the position of each
object in this space by hashing all of the object's attribute values to
determine a spatial coordinate.

.. image:: _static/axis.png
    :align: center
    :width: 200pt

For example a table of objects with "first name", "last name" and "phone
number" attributes corresponds to a three dimensional hyperspace where each
dimension corresponds to one object attribute. Such a space is depicted in the
diagram above. In this space, there exist three objects. The red point is
"John Smith" whose phone number is 555-8000. The green point is "John Doe"
whose phone number is 555-7000. The blue point is "Jim Bob" whose phone number
is 555-2000. Anyone named "John" must map to somewhere in the yellow plane.
Similarly, anyone with the last name "Smith" must map to somewhere within the
translucent plane. Naturally, all people named "John Smith" must map to
somewhere along the line where these two planes intersect.

In each multi-dimnesional space corresponding to a type of object, HyperDex
assigns nodes to disjoint regions of the space, which we call zones. The
example figure shows two of these assignments. Notice that the line for "John
Smith" only intersects two out of the eight zones. Consequently, performing a
search for all phone numbers of "John Smith" requires contacting only two
nodes. Furthermore, the search could be made more specific by restricting it to
all people named "John Smith" whose phone number falls between 555-5000 and
555-9999. Such a search contacts only one out of the eight servers in this
hypothetical deployment.

Subspaces
---------
