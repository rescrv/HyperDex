package hyperclient;

import java.util.*;

class MapOpAdd extends MapOp
{
    public MapOpAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, int attrs_sz,
              SWIGTYPE_p_int rc_int_ptr)
    {
        return client.map_add(space, key, attrs, attrs_sz, rc_int_ptr);
    }
}
