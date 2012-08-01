package hyperclient;

import java.util.*;
import java.io.*;

public class DeferredFromAttrs extends Deferred
{
    private boolean comparing = false;

    public DeferredFromAttrs(HyperClient client, SimpleOp op, Object space, Object key,
                                                                        Map map)
                                                            throws HyperClientException,
                                                                    TypeError,
                                                                    MemoryError
    {
        super(client);

        hyperclient_attribute attrs = null;
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
            if ( attrs != null ) HyperClient.free_attrs(attrs,attrs_sz);
        }
    }

    void setComparing()
    {
        comparing = true;
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if ( comparing && status() == hyperclient_returncode.HYPERCLIENT_CMPFAIL )
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }
}
