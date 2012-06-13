package hyperclient;

import java.util.*;

abstract class MapOp
{
    protected HyperClient client;

    public MapOp(HyperClient client)
    {
        this.client = client;
    }

    abstract long call(String space, String key,
              hyperclient_map_attribute attrs, int attrs_sz,
              SWIGTYPE_p_int rc_int_ptr);
}
