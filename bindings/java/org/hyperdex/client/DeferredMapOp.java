package org.hyperdex.client;

import java.util.*;
import java.io.*;

public class DeferredMapOp extends Deferred
{
    public DeferredMapOp(Client client, MapOp op, Object space, Object key,
                                                                        Map map)
                                                            throws HyperDexClientException,
                                                                   TypeError,
                                                                   MemoryError
    {
        super(client);

	    hyperdex_client_map_attribute attrs = null;
	    long attrs_sz = 0;
	
        try
        {
	        attrs = client.dict_to_map_attrs(map);
	        attrs_sz = attrs.getAttrsSz(); // Can't use map.size(). It's been
                                           // flattened to the larger cardinality
                                           // of map operands.
	
	        reqId = op.call(client.getBytes(space,true),
                            client.getBytes(key),
                            attrs, attrs_sz, rc_ptr);

            checkReqIdKeyMapAttrs(reqId, status(), attrs, attrs_sz);

	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( attrs != null ) Client.free_map_attrs(attrs,attrs_sz);
        }
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }
}
