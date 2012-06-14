package hyperclient;

import java.util.*;

class SimpleOpPut extends SimpleOp
{
    public SimpleOpPut(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_int rc_int_ptr)
    {
        return client.put(space, key, attrs, attrs_sz, rc_int_ptr);
    }
}
