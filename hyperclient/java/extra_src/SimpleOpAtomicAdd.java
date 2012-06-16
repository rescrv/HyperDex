package hyperclient;

import java.util.*;

class SimpleOpAtomicAdd extends SimpleOp
{
    public SimpleOpAtomicAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return hyperclient.hyperclient_atomic_add(client.get_hyperclient(),
                                           space,
                                           key.getBytes(),
                                           attrs, attrs_sz,
                                           rc_ptr);
    }
}
