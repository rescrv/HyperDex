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
   dimensions username, first, last, hobbies (set(string)), pending_requests (list(string)), unread_messages (map(string,string)) 
   key username auto 0 3
   subspace first, last auto 0 3
   EOF

This replicates data three times, allowing for up to two failures at any given
time.


Lists, Sets, and Maps
---------------------

In the previous section, we created a space for our rudimentary social
networking application. This space consists of both basic user attributes
(namely username, firstname, and lastname), all of which are simple strings.
But in complex web applications, there will typically be more complex data
that we will want to keep in some kind of a data structure. In this instance,
let's see how we would use HyperDex to store three complex datastructures; namely, a set
of hobbies, a list of pending friend requests, and a map mapping usernames to
unread messages. 

Let's go over how to use these datastructure attributes. We first create 
create a user for this example:

.. sourcecode:: pycon

   >>> c.put('socialnetwork', 'jsmith1', {'first' : 'John', 'last' : 'Smith', 
   ...                                    'hobbies' : set(['']), 
   ...                                    'pending_requests': [''], 
   ...                                    'unread_messages' : {'' : ''}})


Note that datastructures currently need to be initialized with an empty entry
in the Python client. 

Using the list datastructure, we can add a new unread messages to user ``jsmith1``
by issuing the following operation:

.. sourcecode:: pycon

   >>> c.list_rpush('socialnetwork', 'jsmith1', {'pending_requests' : 'bjones1'})
   True
   >>> c.get('socialnetwork', 'jsmith1')['pending_requests']
   ['', 'bjones1']

The operation ``list_rpush`` is guaranteed to be performed atomically. We do
not need to provide additional synchronization when manipulating HyperDex's
datastructure types. Let's flesh out the user ``jsmith1`` with some hobbies and
unread messages and create user ``bjones1``.

.. sourcecode:: pycon

   >>> c.set_union('socialnetwork', 'jsmith1', {'hobbies' : 
   ...             set(['hockey', 'basket weaving', 'hacking', 
   ...                  'air guitar rocking'])})
   True
   >>> c.get('socialnetwork', 'jsmith1')['hobbies']
   set(['hacking', 'air guitar rocking', 'hockey', 'basket weaving'])
   >>> c.map_add('socialnetwork', 'jsmith1', 
   ...           {'unread_messages' : {'bjones1' : 'Hi John'}})
   True
   >>> c.map_add('socialnetwork', 'jsmith1', 
   ...           {'unread_messages' : {'tbrown1' : 'Lunch?'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   {'tbrown1': 'Lunch?', 'bjones1': 'Hi John'}
   >>> c.put('socialnetwork', 'bjones1', {'first' : 'Bob', 'last' : 'Jones', 
   ...                                    'hobbies' : set(['hacking', 'swimming']), 
   ...                                    'pending_requests': [''],
   ...                                    'unread_messages' : {'' : ''}})
   >>> True

We can read and clear out ``jsmith1``'s pending requests by issuing  the
following (remember that we need to skip the initial empty entry):

.. sourcecode:: pycon

   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   ['', 'bjones1']
   >>> c.condput('socialnetwork', 'jsmith1', 
                 {'pending_requests' : ['', 'bjones1']}, {'pending_requests' : ['']})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']
   ['']

We can also limit ``jsmith1``'s hobbies to include only those that he shares
with ``bjones1``. Although this is an unusual requirement, one can imagine this
being very useful when generating shared hobbies in a group.

.. sourcecode:: pycon

   >>> c.set_intersect('socialnetwork', 'jsmith1', 
   ...                 {'hobbies' : c.get('socialnetwork', 'bjones1')['hobbies']})
   True
   >>> c.get('socialnetwork', 'jsmith1')['hobbies']
   set(['hacking'])

Finally, we can modify unread messages in-place within the map. For example, let's
prepend ``"Hi John, "`` and append ``" Tom"`` to ``tbrown1``'s message to ``jsmith1``.

.. sourcecode:: pycon

   >>> c.map_string_prepend('socialnetwork', 'jsmith1', 
   ...                      {'unread_messages' : {'tbrown1' : 'Hi John, '}})
   True
   >>> c.map_string_append('socialnetwork', 'jsmith1', 
   ...                      {'unread_messages' : {'tbrown1' : ' Tom'}})
   True
   >>> c.get('socialnetwork', 'jsmith1')['unread_messages']['tbrown1']
   'Hi John, Lunch? Tom'
 
As with the other API methods that we introduced in the previous tutorials, there
are corresponding asynchronous methods for manipulating HyperDex datastructures.
Please see the API documentation for a full listing of both synchronous and 
asynchronous methods.
