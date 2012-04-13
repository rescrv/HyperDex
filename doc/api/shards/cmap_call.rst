On success, the integer returned will be a positive integer unique to this
request.  The request will be considered complete when
:c:func:`hyperclient_loop` returns the same ID.  If the integer returned is
negative, it indicates an error generating the request, and ``*status``
contains the reason why.  ``HYPERCLIENT_UNKNOWNATTR``,
``HYPERCLIENT_WRONGTYPE`` and ``HYPERCLIENT_DUPEATTR`` indicate which
attribute caused the error by returning ``-1 - idx_of_bad_attr``.

client:
   An initialized :c:type:`hyperclient` instance.

space:
   A NULL-terminated C-string containing the name of the space to retrieve
   the object from.

key:
   A sequence of bytes to be used as the key.  This pointer must remain valid
   for the duration of the call.

key_sz:
   The number of bytes pointed to by :c:data:`key`.

attrs:
   The set of attributes to be changed.  Each struct will indicate a map on
   which to operate with :c:member:`hyperclient_attribute.attr` and the key
   within that map with :c:member:`hyperclient_attribute.map_key`.  The
   :c:member:`hyperclient_attribute.value` field will be used to atomically
   modify the specified attribute, thus allowing several attributes to be
   changed in different ways.  This pointer must remain valid for the duration
   of the call.

attrs_sz:
   The number of attributes pointed to by :c:data:`attrs`.

status:
   A return value in which the result of the operation will be stored.  If
   this function returns successfully, this pointer must remain valid until
   :c:func:`hyperclient_loop` returns the same ID returned by this function.
