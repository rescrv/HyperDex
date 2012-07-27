package hyperclient;

import java.util.*;
import java.io.*;

public class DeferredMapOp extends Deferred
{
    public DeferredMapOp(HyperClient client, MapOp op, byte[] space, byte[] key,
                                                                        Map map)
                                                            throws HyperClientException,
                                                                   TypeError,
                                                                   MemoryError
    {
        super(client);

	    hyperclient_map_attribute attrs = null;
	    long attrs_sz = 0;
	
        try
        {
	        attrs = HyperClient.dict_to_map_attrs(map);
	        attrs_sz = attrs.getAttrsSz(); // Can't use map.size(). It's been
                                           // flattened to the larger cardinality
                                           // of map operands.
	
	        reqId = op.call(space, key, attrs, attrs_sz, rc_ptr);

            checkReqIdKeyMapAttrs(reqId, status(), attrs, attrs_sz);

	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( attrs != null ) HyperClient.free_map_attrs(attrs,attrs_sz);
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
