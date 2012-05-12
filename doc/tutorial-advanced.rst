Advanced Tutorial
=================

HyperDex offers advanced features beyond the simple ``GET``/``PUT``/``SEARCH``
API presented in the basic tutorial.  If you haven't checked out that tutorial
yet, it's a great starting point as this tutorial builds upon it.  The HyperDex
client provides more advanced features, such as asynchronous operations, atomic
read-modify-write operations, and transparent fault tolerance.  These advanced
features extract more performance and enable richer applications without
sacrificing consistency.

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
Finally, we create a space which makes use of all three systems in the cluster:

.. sourcecode:: console

   $ hyperdex-coordinator-control --host 127.0.0.1 --port 6970 add-space << EOF
   space phonebook
   dimensions username, first, last, phone (int64)
   key username auto 0 3
   subspace first, last, phone auto 0 3
   EOF

This replicates data three times, allowing for up to two failures at any given
time.

Asynchronous Operations
-----------------------

In the previous tutorial, we submitted *synchronous* operations to the key-value
store, where the client had just a single outstanding request and waited
patiently for that request to complete. In high-throughput applications, clients
may have a batch of operations they want to perform on the key-value store. The
standard practice in such cases is to issue *asynchronous* operations, where the
client does not immediately wait for each individual operation to complete.
HyperDex has a very versatile interface for supporting this use case.

Asynchronous operations allow a single client library to achieve higher
throughput by submitting multiple simultaneous requests in parallel. Each
asynchronous operation returns a small token that identifies the outstanding
asynchronous operation, which can then be used by the client, if and when
needed, to wait for the completion of selected asynchronous operations.

Every API method we've covered so far in the tutorials (e.g. ``get``) has a
corresponding ``async_*`` version (e.g. ``async_get``) for performing
asynchronous operations.  The basic pattern of usage for asynchronous operations
is:

 * Initiate the asynchronous operation
 * Do some work and perhaps issue more operations, async or otherwise,
 * Wait for selected asynchronous operations to complete

This enables the application to continue doing other work while HyperDex
performs the requested operations.  Here's how we could insert the
"jsmith1" user asynchronously:

.. sourcecode:: pycon

   >>> d = c.async_put('phonebook', 'jsmith1',
   ...                 {'first': 'John', 'last': 'Smith', 'phone': 6075551024})
   >>> d
   <hyperclient.DeferredInsert object at 0x7f2bbc3252d8>
   >>> do_work()
   >>> d.wait()
   True
   >>> d = c.async_get('phonebook', 'jsmith1')
   >>> d.wait()
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}

Notice that the return value of the first ``d.wait()`` is ``True``.  This is the
same return value that would have come from performing ``c.put(...)``, except
the client was free to do other computations while HyperDex servers were
processing the ``put`` request.  Similarly, the second asynchronous operation,
``async_get``, queues up the request on the servers, frees the client to
perform other work, and yields its results only when ``wait`` is called.

By itself, an asynchronous operation is not very useful if it is waited on right
away.  The true power comes from requesting multiple concurrent operations:

.. sourcecode:: pycon

   >>> d1 = c.async_put('phonebook', 'jd',
   ...                  {'first': 'John', 'last': 'Doe', 'phone': 6075557878})
   >>> d2 = c.async_get('phonebook', 'jsmith1')
   >>> d1.wait()
   True
   >>> d2.wait()
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}

Note that the order in which operations are waited on does not matter.  We could
just as easily execute them in a different order, and still get the desired
effect:

.. sourcecode:: pycon

   >>> d1 = c.async_put('phonebook', 'jd',
   ...                  {'first': 'John', 'last': 'Doe', 'phone': 6075557878})
   >>> d2 = c.async_get('phonebook', 'jsmith1')
   >>> d2.wait()
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}
   >>> d1.wait()
   True

This allows for powerful applications.  For instance, it is possible to issue
thousands of requests and then wait for each one in turn without having to
serialize the round trips to the server.

Note that HyperDex may choose to execute concurrent asynchronous operations in
any order.  It's up to the programmer to order requests by calling ``wait``
appropriately.

Atomic Read-Modify-Write Operations
-----------------------------------

Atomic read-modify-write operations enable concurrent applications that would
otherwise be impossible to implement correctly. For instance, an application
which performs a ``GET`` request followed by a ``PUT`` to the same key is not
guaranteed to have these two requests operate immediately back to back, as other
clients may issue requests to the key in the mean time.

The canonical example here involves two clients who are both trying to update a
salary field. One is trying to deduct taxes -- let's assume that they are
hard-working academics being taxed at the maximum rate of 36%, not the cushy 15%
that people on Wall Street seem to pay.  The other client is trying to add a
$1500 teaching award to the yearly salary. So one client will be doing
v1=GET(salary), v1 = v1 - 0.36*v1; PUT(salary, v1). The other client will be
doing v2=GET(salary), v2 += 1500; PUT(salary, v2), where v1 and v2 are variables
local to each client. Since these GET and PUT operations can be interleaved in
any order, it is possible for the clients to succeed (so both the deduction and
the raise are issued) and yet for the salary to not reflect the results! If the
sequence is GET from client1/GET from client2/PUT from client2/PUT from client1,
the raise will be overwritten. We certainly cannot have that!

Atomic read-modify-write operations provide a solution to this problem.  Such
operations are guaranteed to execute without interference from other operations.
The operation ensures that the read-modify-write sequence that comprises the
operation is executed in a manner that cannot be interrupted by or interleaved
with any other operation. The entire block is one atomic unit.

HyperDex supports a few different types of atomic instructions. Perhaps the most
general one is the ``condput``.  A ``condput`` performs an ``put`` if and only
if the value being updated matches a condition specified along with the new
values to be inserted.

Since our sample database was set up with phone numbers, let's do some examples
involving phone record updates. Let's say that the application wants to update
John Smith's phone number, but wants the application to fail if the application
has changed the phone number since it was last read:

.. sourcecode:: pycon

   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}
   >>> c.condput('phonebook', 'jsmith1',
   ...           {'phone': 6075551024}, {'phone': 6075552048})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552048}

Here, we told HyperDex to update John's phone number to end in 607-555-2048 if
and only if it is currently equal to 607-555-1024. The third argument specified
a set of object attributes that must match the object for the update to succeed.
The fourth argument specified the set of new values to insert into the object in
case of a match.

Not surprisingly, this request succeeded, as John's phone number matched the
specified values. Let's try issuing the same operation again.

.. sourcecode:: pycon

   >>> c.condput('phonebook', 'jsmith1',
   ...           {'phone': 6075551024}, {'phone': 6075552048})
   False

Notice that ``condput`` failed because the value of the phone number
field is no longer 6075551024.

Note that the last argument has the same generality as the arguments to a
regular ``put`` operation. So there is no requirement that a
``condput`` check and update the same field. The following is a
perfectly legitimate operation that updates the first name field of object with
key "jsmith" to "James" if the phone number has not changed:

.. sourcecode:: pycon

   >>> c.condput('phonebook', 'jsmith1',
   ...           {'phone': 6075552048}, {'first': 'James'})
   True

The great thing about HyperDex is that ``condput`` operations are
fast.  In fact, their performance is indistinguishable from a normal ``put``,
all else being equal.  Thus, you can rely heavily upon ``condput``
operations to avoid race conditions without sacrificing performance.

That's not all HyperDex offers in the way of atomic operations.  In many
applications, the clients will want to increment or decrement a numerical field.
For instance, Google +1 and Reddit-style up/down-vote services will want to
perform such arithmetic atomically. The way we set up our space and data for
this example is not a good match for a good example, but let's pretend that John
Smith has switched offices, and the application knows that this simply
increments his phone number by 1.  We could accomplish this with the following:

.. sourcecode:: pycon

   >>> c.atomic_add('phonebook', 'jsmith1', {'phone': 1})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552049}
   >>> c.atomic_sub('phonebook', 'jsmith1', {'phone': 1})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552048}

Notice that each of these changes requires just one request to the server.

We can increment or decrement by any signed 64-bit value:

.. sourcecode:: pycon

   >>> c.atomic_add('phonebook', 'jsmith1', {'phone': 10})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552058}
   >>> c.atomic_sub('phonebook', 'jsmith1', {'phone': 10})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075552048}

Keep in mind that ``condput`` operations can and will fail, as intended, if
there are interceding operations that update the object fields that must match.
In these cases, the client will typically want to re-fetch the object,
re-perform its updates, and re-submit the conditional operation.

Of course, it is perfectly reasonable to issue atomic operations asynchronously,
as discussed in the preceding section. One would need to just use the ``async_``
prefix to the operations.

Fault Tolerance
---------------

HyperDex handles failures automatically.  When a node fails, HyperDex will
detect the failure, repair the subspace by eliminating the failed nodes from the
value-dependent chains it is using to propagate values, and will automatically
reintegrate any spare nodes into the space to restore the desired level of fault
tolerance.

Let's see this in action by killing some nodes and checking what happens to our
data:

.. sourcecode:: pycon

   >>> c.put('phonebook', 'jsmith1', {'phone': 6075551024})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}

So now we have a data item we deeply care about. We certainly would not want our
NoSQL store to lose this data item because of a failure. Let's create a failure
by killing one of the three hyperdaemon processes we started in the setup phase
of the tutorial. Feel free to use "kill -9", there is no requirement that the
nodes shut down in an orderly fashion. HyperDex is designed to handle crash
failures.

.. sourcecode:: pycon

   >>> # kill a node at random
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551024}
   >>> c.put('phonebook', 'jsmith1', {'phone': 6075551025})
   True
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551025}
   >>> c.put('phonebook', 'jsmith1', {'phone': 6075551026})
   True

So, our data is alive and well. Not only that, but the subspace is continuing to
operate as normal and handling updates at its usual rate.

Let's kill one more server.

.. sourcecode:: pycon

   >>> # kill a node at random
   >>> c.get('phonebook', 'jsmith1')
   Traceback (most recent call last):
   File "<stdin>", line 1, in <module>
   File "hyperclient.pyx", line 473, in hyperclient.Client.put ...
   File "hyperclient.pyx", line 499, in hyperclient.Client.async_put ...
   File "hyperclient.pyx", line 255, in hyperclient.DeferredInsert.__cinit__ ...
   hyperclient.HyperClientException: Connection Failure
   >>> c.get('phonebook', 'jsmith1')
   {'first': 'John', 'last': 'Smith', 'phone': 6075551026}

Note that the HyperDex API exposes some failures to the clients at the moment,
so a client may have to catch HyperClientException and retry the operation. We
plan to hide this behavior in the bindings in the near future, but for now, this
is the behavior of the system.

In this example, behind the scenes, there were two node failures in the
triply-replicated space. Each failure was detected, the space was repaired by
cleaving out the failed node, and normal operations resumed without data loss.
