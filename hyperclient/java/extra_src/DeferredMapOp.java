package hyperclient;

import java.util.*;
import java.math.*;

public class DeferredMapOp extends Deferred
{
    public DeferredMapOp(HyperClient client, MapOp op, String space, String key,
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

            BigInteger idx_bi = BigInteger.ONE.subtract(
                          BigInteger.valueOf(reqId));

            long idx = idx_bi.longValue();
	
	        if (reqId < 0)
	        {
                if ( attrs != null && idx_bi.compareTo(BigInteger.ZERO) >= 0
                                   && idx_bi.compareTo(attrs.getAttrsSz_bi()) < 0 )
                {
                    hyperclient_map_attribute hma = HyperClient.get_map_attr(attrs,idx);
                    throw new HyperClientException(status(),hma.getMapAttrName());
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
            if ( attrs != null ) HyperClient.destroy_map_attrs(attrs,attrs_sz);
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
