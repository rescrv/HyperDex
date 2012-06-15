package hyperclient;

import java.util.*;

abstract class SimpleOp
{
    protected HyperClient client;

    public SimpleOp(HyperClient client)
    {
        this.client = client;
    }

    abstract long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr);
}
