package hyperclient;

import java.util.*;
import java.io.*;

public class DeferredFromAttrs extends Deferred
{
    public DeferredFromAttrs(HyperClient client, SimpleOp op, byte[] space, byte[] key,
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
            attrs = HyperClient.dict_to_attrs(map);
	
	        reqId = op.call(space, key, attrs, attrs_sz, rc_ptr);
	
            checkReqIdKeyAttrs(reqId, status(), attrs, attrs_sz);

	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( attrs != null ) HyperClient.free_attrs(attrs,attrs_sz);
        }
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();
        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }
}
