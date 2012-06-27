

Basic Operations
================

A HyperDex cluster consists of three types of components: clients, servers, and
a coordinator.

Applications built on top of HyperDex are the clients. They use the HyperDex API
through various bindings for popular languages (e.g. C, C++, and Python) to
issue operations, such as GET, PUT, SEARCH, DELETE, etc, to the storage system.

The HyperDex servers are responsible for storing the data in the system. You can
have a cluster with as few as just a single server, though typical installations
will have dozens to hundreds of servers. These servers store the data in memory
and on disk, and they can crash at any time. HyperDex can be configured to
tolerate as many failed nodes as desired.

The HyperDex coordinator maintains the "hyperspace." This involves making sure
that servers are up, detecting failed or slow nodes, taking them out of the
system, and replacing them where necessary. The coordinator maintains a critical
data structure, the hyperspace map, that establishes the mapping between the
hyperspace and servers. Clients use this map to locate the servers they need to
contact, while servers use it to perform object propagation and replication to
achieve the application's desired goals.

In this tutorial, we'll discuss how to get a HyperDex cluster up and running. In
particular, we'll create a simple space, insert objects into it, retrieve those
objects, and then perform queries over these objects.

Starting the Coordinator
------------------------

First, we need to start up a coordinator that will oversee the organization of
the hyperspace.

The following command starts the coordinator:

.. sourcecode:: console

   $ hyperdex-coordinator --control-port 6970 --host-port 1234 --logging debug

The coordinator has a control-port over which we can instruct it to rearrange
the hyperspace and a host-port over which it communicates with server nodes.
While a regular user should never have to interact with either of these ports
directly, those of you who like to hack can always fire up a telnet session to
the control port and issue commands directly to the coordinator.

At this point, the coordinator is up and running, and we're ready to start up
additional nodes in our cluster.

Starting HyperDex Daemons
-------------------------

The HyperDex servers are the workhorse processes that actually house the data in
the data store and respond to client requests. Let's start a server on the the
same machine as the coordinator:

.. sourcecode:: console

   $ hyperdex-daemon --host 127.0.0.1 --port 1234 --bind-to 127.0.0.2 --data /path/to/data

The first two arguments are the IP and host port of the controller we started
earlier. This enables the server to announce its presence, which in turn enables
the coordinator to assign a zone to the newly arrived server. At that point, the
server can take over some of the data load from an existing server, participate
in the data propagation protocol, and start handling client requests. But since
we have no data yet and this is our first node, this particular server does not
have much work to do.

Note that the third argument (127.0.0.2) is the IP address to which we want to
bind the server node. We are using a loopback address here for the purposes of
the tutorial, while in real life you will undoubtedly want to use one of the IP
addresses bound to your internal network.

The last argument is a pointer to a directory where the server will store all
data.

It doesn't hurt to start a few more server instances at this point (although it
is not required to continue with the tutorial).

You now have a functional HyperDex cluster.  It's time to do something with it.

Creating a new Space
--------------------

Let's imagine that we are building a phone book application.  Such a phonebook
would end up keeping track of people's first name, last name, and phone number.
And to distinguish unique users, it might assign a user id to each user. Let's
see how we can instruct HyperDex to create a suitable space for holding such
objects:

.. sourcecode:: console

   $ hyperdex-coordinator-control --host 127.0.0.1 --port 6970 add-space << EOF
   space phonebook
   dimensions username, first, last, phone (int64)
   key username auto 1 3
   subspace first, last, phone auto 2 3
   EOF

This command creates a new space called ``phonebook``. Since we have four
attributes (username, firstname, lastname, phone number) per object, we have
chosen to create a four-dimensional space. Typically, we will assign a separate
dimension to each searchable attribute, though we can assign fewer dimensions if
desired. The resulting four-dimensional space is hard to visualize, but you can
see why the name HyperDex is so apt.

Note that, under the covers, HyperDex will not necessarily create one giant
hyperdimensional space. Doing so would cause lots of problems when trying to map
objects with large numbers of attributes. Instead, we will typically want to
create *subspace* consisting of smaller numbers of dimensions. The lower number
of dimensions enable the mapping from points in space to nodes in the cluster to
be more efficient; in particular, fewer nodes need to be contacted during search
operations. In this simple example, we will want to create a 1-dimensional
subspace for the object key so object lookups using just a single key can be
resolved to a single host immediately, and a 3-dimensional subspace for the rest
of the attributes. Let's see how this is done.

The ``key`` line designates the  ``username`` attribute to be the key under
which objects are stored and retrieved. The key plays a special role in
HyperDex, though it's different from the role keys play in other NoSQL systems.
In other NoSQL systems, objects can _only_ be retrieved by the key under which
they were inserted.  So an object ``<rescrv, Robert, Escriva, 555-1212>`` can
only be retrieved by its key ``rescrv``. In HyperDex, we will be able to perform
retrievals for all Roberts or Escrivas or, even, reverse lookups by the phone
number. The key simply serves as an object identifier such that updates to the
object (e.g. changes to the phone number or name) are sequenced and handled
consistently.

Since large scale cloud-computing deployments are sure to encounter failures, we
will want to safeguard the data in our key-value store by creating replicas.
The ``1 3`` at the end of the key line instructs the system to automatically
divide the key subspace into ``pow(2, 1)`` zones and to replicate each zone on
three nodes. Likewise, the subspace of the ``first``, ``last`` and ``phone``
attributes will be divided into ``pow(2, 2)`` zones.  Unless you started
multiple servers earlier, each zone will only be replicated once.

As a general rule, we will want to automatically partition the hyperspace into a
number of zones which is a power of two that is not significantly greater than
the number of nodes in the cluster.  A replication value of 0 does not make
sense (what does it mean to have 0 replicas? we should just delete the item if
we do not want it stored), 1 is fine for soft-state, and any value greater than
1 will enable us to tolerate failures in our server ensemble.

Interacting with the ``phonebook`` Space
----------------------------------------

Now that we have our hyperspace defined and ready to go, it's time to insert
some information into our ``phonebook``.

First, let's connect to HyperDex:

.. sourcecode:: pycon

   >>> import hyperclient
   >>> c = hyperclient.Client('127.0.0.1', 1234)

This line instructs the client bindings to talk to the controller and get the
current hyperspace configuration.  There is no need for static configuration
files. Clients always receive the most up-to-date configuration (and if the
configuration changes, say, due to failures, the servers will detect that a
client is operating with an out-of-date configuration and instruct it to retry
with a fresh config).

Now that we have a workable client, we can put an object onto the servers:

.. sourcecode:: pycon

   >>> c.put('phonebook', 'jsmith1', {'first': 'John', 'last': 'Smith',
   ...                                'phone': 6075551024})
   True

This operation will determine the right spot in the hyperspace for this object,
contact the servers responsible, and issue the ``put`` operation. The operation
will only return once the object has been committed at all requisite nodes.

Now that we have an object in the phonebook, we can easily retrieve the
``jsmith1`` object by using a standard ``get``:

.. sourcecode:: pycon

   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}

Yay, we inserted an object under the key ``jsmith1`` and retrieved it using the
same key.  This looks exactly like every other NoSQL store out there, but there
are a few differences.

First, it's blazingly fast. You can look in our latest performance graphs for
the precise comparisons, but typically, HyperDex is just way faster than other
key-value stores.

Second, it's fault-tolerant. When we performed the ``put``, our operation was
sent through a *value-dependent chain* of servers assigned to a particular
point. The client received an acknowledgment only when the object was replicated
on every single server in the chain. Unlike NoSQL stores that optimistically
assume that an update was committed when it's in the send buffer of a single
client (we're looking at you MongoDB), or when it's in the filesystem cache of a
single server (we're looking at you Cassandra), HyperDex responds only when all
the servers have been updated. And we can pick our replication levels to achieve
any level of fault-tolerance we desire.

Finally, it's consistent. If we had multiple concurrent ``put`` operations
being issued by multiple clients at the same time, we would never see an
inconsistent state.  What is an inconsistent state?  It's what you get when you
settle for *eventual consistency*.  For instance, we would not want a
prescription tracking system to say that we dispensed a drug, then to say we did
not, only to settle on (say) having dispensed it. Yet this is precisely what
might happen with an eventually consistent NoSQL key-value store. Eventual
consistency is no consistency at all. In contrast, HyperDex provides
linearizability. Time will never roll backwards from the point of any client.

And it gets better. For we can not only retrieve objects by their key, but we
can also retrieve them when we don't know their key. Here are some examples:

.. sourcecode:: pycon

   >>> [x for x in c.search('phonebook', {'first': 'John'})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]
   >>> [x for x in c.search('phonebook', {'last': 'Smith'})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]

Let's do that reverse phone number search:

.. sourcecode:: pycon

   >>> [x for x in c.search('phonebook', {'phone': 6075551024})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]

Here's a fully-qualified search. Hyperspace hashing makes this nearly as fast as
a key-based lookup:

.. sourcecode:: pycon

   >>> [x for x in c.search('phonebook',
   ...  {'first': 'John', 'last': 'Smith', 'phone': 6075551024})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]

Let's add another user named "John Doe":

.. sourcecode:: pycon

   >>> c.put('phonebook', 'jd', {'first': 'John', 'last': 'Doe', 'phone': 6075557878})
   True
   >>> [x for x in c.search('phonebook',
   ...  {'first': 'John', 'last': 'Smith', 'phone': 6075551024})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]
   >>> [x for x in c.search('phonebook', {'first': 'John'})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'},
    {'first': 'John', 'last': 'Doe', 'phone': 6075557878, 'username': 'jd'}]
   >>> [x for x in c.search('phonebook', {'last': 'Smith'})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]
   >>> [x for x in c.search('phonebook', {'last': 'Doe'})]
   [{'first': 'John', 'last': 'Doe', 'phone': 6075557878, 'username': 'jd'}]

Should John Doe decide he no longer wants to be listed in the phonebook, it's
trivial to remove his listing:

.. sourcecode:: pycon

   >>> c.delete('phonebook', 'jd')
   True
   >>> [x for x in c.search('phonebook', {'first': 'John'})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075551024, 'username': 'jsmith1'}]

Suppose John Smith needs to change his phone number. This is easily accomplished
by specifying just the key for the object and the changed attribute.  All other
attributes will be preserved (or be blank in the case where the object doesn't
exist).

.. sourcecode:: pycon

   >>> c.put('phonebook', 'jsmith1', {'phone': 6075552048})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552048}

Smith is a popular name.  Let's say there was "John Smith" from Rochester (area
code 585):

.. sourcecode:: pycon

   >>> c.put('phonebook', 'jsmith2',
   ...          {'first': 'John', 'last': 'Smith', 'phone': 5855552048})
   True
   >>> c.get('phonebook', 'jsmith2')
   {'first': 'John', 'last': 'Smith', 'phone': 5855552048}

Suppose we want to locate everyone named "John Smith" from Ithaca (area code
607). We can do this with a range query in HyperDex.

.. sourcecode:: pycon

   >>> [x for x in c.search('phonebook',
   ...  {'last': 'Smith', 'phone': (6070000000, 6080000000)})]
   [{'first': 'John', 'last': 'Smith', 'phone': 6075552048, 'username': 'jsmith1'}]
