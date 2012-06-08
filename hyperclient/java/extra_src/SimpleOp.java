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
              hyperclient_attribute attrs, int attrs_sz,
              SWIGTYPE_p_int rc_int_ptr);
}
