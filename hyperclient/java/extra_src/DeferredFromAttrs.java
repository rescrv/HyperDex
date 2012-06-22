package hyperclient;

import java.util.*;

public class DeferredFromAttrs extends Deferred
{
    public DeferredFromAttrs(HyperClient client, SimpleOp op, String space, String key,
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
	
	        if (reqId < 0)
	        {
                int idx = (int)(-1 - reqId);

                if ( attrs != null && idx >= 0 && idx < attrs_sz )
                {
                    hyperclient_attribute ha = HyperClient.get_attr(attrs,idx);
                    throw new HyperClientException(status(),ha.getAttrName());
                }
                else
                {
                    throw new HyperClientException(status());
                }
	        }
	
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
