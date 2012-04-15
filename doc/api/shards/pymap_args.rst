space:
   A string naming the space in which the object will be inserted.

key:
   The key of the object.  Keys may be either byte strings or integers.

attrs:
   A nested dictionary of map keys to be changed.  Each key in the outter
   dictionary sections a map attribute.  Keys in the inner dictionaries
   correspond to map keys on which to operate.  Values in the inner dictionary
   will be used to modify the specified map key, thus allowing several
   attributes to be changed in different ways.  If the key does not exist in the
   map, it wil be created.
