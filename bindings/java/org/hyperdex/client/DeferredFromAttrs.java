package org.hyperdex.client;

import java.util.*;
import java.io.*;

public class DeferredFromAttrs extends Deferred
{
    private boolean comparing = false;

    public DeferredFromAttrs(Client client, SimpleOp op, Object space, Object key,
                                                                        Map map)
                                                            throws HyperDexClientException,
                                                                    TypeError,
                                                                    MemoryError
    {
        super(client);

        hyperdex_client_attribute attrs = null;
        long attrs_sz = 0;

        try
        {
            attrs_sz = map.size();
            attrs = client.dict_to_attrs(map);
	
	        reqId = op.call(client.getBytes(space,true),
                            client.getBytes(key),
                            attrs, attrs_sz, rc_ptr);
	
            checkReqIdKeyAttrs(reqId, status(), attrs, attrs_sz);

	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( attrs != null ) Client.free_attrs(attrs,attrs_sz);
        }
    }

    void setComparing()
    {
        comparing = true;
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if ( comparing && status() == hyperdex_client_returncode.HYPERDEX_CLIENT_CMPFAIL )
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }
}
