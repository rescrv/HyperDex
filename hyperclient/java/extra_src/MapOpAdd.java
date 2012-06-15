package hyperclient;

import java.util.*;

class MapOpAdd extends MapOp
{
    public MapOpAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_add(space, key, attrs, attrs_sz, rc_ptr);
    }
}
