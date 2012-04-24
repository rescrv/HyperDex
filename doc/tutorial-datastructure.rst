Datastructure Tutorial
======================

As we have seen from the basic and advanced tutorials, HyperDex offers support
for both the basic ``GET``/``PUT``/``SEARCH`` primitives, as well as advanced
features, such as asynchronous and atomic read-modify-write operations, that
are critical to enabling more sophisticated and demanding applications.  So
far, we have only illustrated HyperDex's features using strings and
integers.  In this tutorial, we explore HyperDex's richer datastructures:
lists, sets, and maps. We will show that, by providing efficient atomic
operations on these rich, native datastructures, HyperDex can greatly simplify
space design for applications with complicated data layout requirements.

Setup
-----

The setup for this tutorial is very similar to that in the basic tutorial.
First, we launch the coordinator:

.. sourcecode:: console

   $ hyperdex-coordinator --control-port 6970 --host-port 1234 --logging debug

Next, let's launch a few daemon processes.  Execute the following commands (note
that each instance has a different ``/path/to/data``, as every node will be
storing a different portion of the hyperspace (aka shard)):

.. sourcecode:: console

   $ hyperdex-daemon --host 127.0.0.1 --port 1234 --bind-to 127.0.0.2 --data /path/to/data1
   $ hyperdex-daemon --host 127.0.0.1 --port 1234 --bind-to 127.0.0.2 --data /path/to/data2
   $ hyperdex-daemon --host 127.0.0.1 --port 1234 --bind-to 127.0.0.2 --data /path/to/data3

This brings up three different daemons ready to serve in the HyperDex cluster.
Finally, we create a space which makes use of all three systems in the cluster.
In this example, let's create a space that may be suitable for serving data in a
social network:

.. sourcecode:: console

   $ hyperdex-coordinator-control --host 127.0.0.1 --port 6970 add-space << EOF
   space socialnetwork
   dimensions username, first, last,
              pending_requests (list(string)),
              hobbies (set(string)),
              unread_messages (map(string,string))
   key username auto 0 3
   subspace first, last auto 0 3
   EOF

This space captures a user's profile including pending friend requests, hobbies
and unread private messages.  The data is replicated to tolerate up to two
failures at any given time.

Lists
-----

We've created a space for a user's profile in our social networking app.  Let's
add support for friend requests.  For this we'll use the ``pending_requests``
attribute in the ``socialnetwork`` space.

To start, let's create a profile for John Smith:

.. sourcecode:: pycon

   >>> c.put('socialnetwork', 'jsmith1', {'first': 'John', 'last': 'Smith'})
   True
   >>> c.get('socialnetwork', 'jsmith1')
   {'first': 'John', 'last': 'Smith',
    'pending_requests': [],
    'hobbies': set([]),
    'unread_messages': {}}

Imagine that shortly after joining, John Smith receives a friend request from
his friend Brian Jones.  Behind the scenes, this could a simple list operation,
pushing the friend request onto John's ``pending_requests``:

.. sourcecode:: pycon

   >>> c.list_rpush('socialnetwork', 'jsmith1', {'pending_requests': 'bjones1'})
   True
   >>> c.get('socialnetwork', 'jsmith1')['pending_requests']
   ['bjones1']

The operation ``list_rpush`` is guaranteed to be performed atomically, and will
be applied consistently with respect to all other operations on the same list.

Sets
----

Our social networking app captures the notion of hobbies.  A set of strings is a
natural representation for a user's hobbies, as each hobby is represented just
once and may be added or removed.

Let's add some hobbies to John's profile:

.. sourcecode:: pycon

   >>> hobbies = set(['hockey', 'basket weaving', 'hacking',
   ...                'air guitar rocking']
   >>> c.set_union('socialnetwork', 'jsmith1', {'hobbies': hobbies})
   True
   >>> c.set_add('socialnetwork', 'jsmith1', {'hobbies': 'gaming'})
   True
   >>> c.get('socialnetwork', 'jsmith1')['hobbies']
   set(['hacking', 'air guitar rocking', 'hockey', 'gaming', 'basket weaving'])

If John Smith decides that his life's dream is to just write code, he may decide
to join a group on the social network filled with like-minded individuals.  Our
social network allows the group to limit members' profiles to just reflect its
interests:

.. sourcecode:: pycon

   >>> c.set_intersect('socialnetwork', 'jsmith1',
   ...                 {'hobbies': set(['hacking', 'programming'])})
   True
   >>> c.get('socialnetwork', 'jsmith1')['hobbies']
   set(['hacking'])

Notice how John's hobbies become the intersection of his previous hobbies, and
the groups approved list of hobbies.

Maps
----

Lastly, our social networking system needs a means for allowing users to
exchange messages.  Let's demonstrate how we can accomplish this with the
``unread_messages`` attribute:

.. sourcecode:: pycon

   >>> c.map_add('socialnetwork', 'jsmith1',
   ...           {'unread_messages' : {'bjones1' : 'Hi John'}})
   True
   >>> c.map_add('socialnetwork', 'jsmith1',
   ...           {'unread_messages' : {'timmy' : 'Lunch?'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   {'timmy': 'Lunch?', 'bjones1': 'Hi John'}

Messages may be modified in-place within the map.  For example, if Brian sent
another message to John, we could separate the messages with "|" and just append
the new message:

.. sourcecode:: pycon

   >>> c.map_string_append('socialnetwork', 'jsmith1',
   ...                      {'unread_messages' : {'bjones' : '| Want to hang out?'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']['tbrown1']
   'Hi John, Lunch? Tom'

Note that maps may have strings or integers as values, and every atomic
operation available for strings and integers is also available in map form,
operating on the values of the map.

Asynchronous Datastructure Operations
-------------------------------------

As with all other API methods in HyperDex, there are corresponding asynchronous
methods for manipulating HyperDex datastructures.  For example, the social
networking application can make an asynchronous call to make friend requests:

.. sourcecode:: pycon

   >>> d = c.async_list_rpush('socialnetwork', 'jsmith1', {'pending_requests': 'timmy'})
   >>> d.wait()
   True
   >>> c.get('socialnetwork', 'jsmith1')['pending_requests']
   ['bjones1', 'timmy']

Please see the API documentation for a full listing of both synchronous and
asynchronous methods.
