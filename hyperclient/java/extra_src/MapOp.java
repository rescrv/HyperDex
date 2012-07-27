package hyperclient;

import java.util.*;

abstract class MapOp
{
    protected HyperClient client;

    public MapOp(HyperClient client)
    {
        this.client = client;
    }

    abstract long call(byte[] space, byte[] key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr);
}
