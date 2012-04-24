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
              upvotes (map(string,int64))
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
    'unread_messages': {},
    'upvotes': {}}

Imagine that shortly after joining, John Smith receives a friend request from
his friend Brian Jones.  Behind the scenes, this could be implemented with a 
simple list operation, pushing the friend request onto John's ``pending_requests``:

.. sourcecode:: pycon

   >>> c.list_rpush('socialnetwork', 'jsmith1', {'pending_requests': 'bjones1'})
   True
   >>> c.get('socialnetwork', 'jsmith1')['pending_requests']
   ['bjones1']

The operation ``list_rpush`` is guaranteed to be performed atomically, and will
be applied consistently with respect to all other operations on the same list.


.. todo::

   XXX Note that lists provide both an lpush and rpush operation. The former
   adds the new element at the head of the list, while the latter adds at the
   tail. They also provide lpop operation for taking an element off the head of
   the list. Coupled together, these operations provide a comprehensive list
   datatype that can be used to implement fault-tolerant lists of all kinds. For
   instnace, one can implement work queues and generalized producer-consumer
   patterns on top of HyperDex lists in a pretty straightforward fashion. In
   this case, producers would push at one end of the list (the tail, with an
   rpush) and consumers would pop from the other (the head, with a pop). Since
   the operations are atomic, no additional synchronization would be necessary,
   enabling a high-performance implementation.


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
to join a group on the social network filled with like-minded individuals.  We can 
use HyperDex's intersect primitive to narrow down his interests:

.. sourcecode:: pycon

   >>> c.set_intersect('socialnetwork', 'jsmith1',
   ...                 {'hobbies': set(['hacking', 'programming'])})
   True
   >>> c.get('socialnetwork', 'jsmith1')['hobbies']
   set(['hacking'])

Notice how John's hobbies become the intersection of his previous hobbies and the 
ones named in the operation.

Overall, HyperDex supports simple set assignment (using the ``put`` interface),
adding and removing elements with :py:meth:`Client.set_add` and
:py:meth:`hyperclient.Client.set_remove`, taking the union of a set with
:py:meth:`hyperclient.Client.set_union` and storing the intersection of a set with
:py:meth:`hyperclient.Client.set_intersect`.

Maps
----

Lastly, our social networking system needs a means for allowing users to
exchange messages.  Let's demonstrate how we can accomplish this with the
``unread_messages`` attribute. In this contrived example, we're going to use an object attribute as a map (aka dictionary) 
to map from a user name to a string that contains the message from that user, as follows:

.. sourcecode:: pycon

   >>> c.map_add('socialnetwork', 'jsmith1',
   ...           {'unread_messages' : {'bjones1' : 'Hi John'}})
   True
   >>> c.map_add('socialnetwork', 'jsmith1',
   ...           {'unread_messages' : {'timmy' : 'Lunch?'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   {'timmy': 'Lunch?', 'bjones1': 'Hi John'}

HyperDex enables map contents to be modified in-place within the map.  For example, if Brian sent
another message to John, we could separate the messages with "|" and just append
the new message:

.. sourcecode:: pycon

   >>> c.map_string_append('socialnetwork', 'jsmith1',
   ...                      {'unread_messages' : {'bjones' : '| Want to hang out?'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   {'timmy': 'Lunch?', 'bjones1': 'Hi John| Want to hang out?'}

Note that maps may have strings or integers as values, and every atomic
operation available for strings and integers is also available in map form,
operating on the values of the map.

For the sake of illustrating maps involving integers, let's imagine that we will use a map to keep track
of the plus-one's and like/dislike's on John's status updates. 

First, let's create some counters that will keep the net count of up and down votes 
corresponding to John's link posts, with ids "http://url1.com" and "http://url2.com". 

.. sourcecode:: pycon

   >>> url1 = "http://url1.com"
   >>> url2 = "http://url2.com"
   >>> c.map_add('socialnetwork', 'jsmith1',
   ...           {'upvotes' : {url1 : 1, url2: 1}})
   True

So John's posts start out with a counter set to 1, as shown above. 

Imagine that two other users, Jane and Elaine, upvote John's first link post,
we would implement it like this:

.. sourcecode:: pycon

   >>> c.map_atomic_add('socialnetwork', 'jsmith1', {'upvotes' : {url1: 1}})
   True
   >>> c.map_atomic_add('socialnetwork', 'jsmith1', {'upvotes' : {url1: 1}})
   True

Charlie, sworn enemy of John, can downvote both of John's urls like this:

.. sourcecode:: pycon

   >>> c.map_atomic_add('socialnetwork', 'jsmith1', {'upvotes' : {url1: -1, url2: -1}})
   True

This shows that any map operation can operate atomically on a group of map attributes at the same time. This is 
fully transactional; all such operations will be ordered in exactly the same way on all replicas, and there is
no opportunity for divergence, even through failures.

Checking where we stand:

.. sourcecode:: pycon

   >>> c.get('socialnetwork', 'jsmith1')['upvotes']
   {'http://url1.com': 2, , 'http://url2.com': 0}

All of the preceding operations could have been issued concurrently -- the results
will be the same because they commute with each other and are executed atomically.

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

Here, we issued an asynchronous operation on a list, waited for it to complete, and
saw that the end result indeed reflected the effect of the asynchronous operation.

So, overall, HyperDex provides a very rich API with complex, aggregate datastructures.
And it supports atomic operations on these datastructures such that concurrent clients 
can use the without the need to coordinate with an external lock server (in fact, if needed,
they can use HyperDex to *implement* a high-performance lock server!). 
