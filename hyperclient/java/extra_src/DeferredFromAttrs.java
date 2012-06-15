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

        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

        try
        {
            attrs = HyperClient.dict_to_attrs(map);
            attrs_sz = map.size();
	
	        reqId = op.call(space, key, attrs, attrs_sz, rc_int_ptr);
	
	        if (reqId < 0)
	        {
	            status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));

                if ( status == ReturnCode.HYPERCLIENT_UNKNOWNATTR
                  || status == ReturnCode.HYPERCLIENT_DUPEATTR // Won't happen
                  || status == ReturnCode.HYPERCLIENT_WRONGTYPE)
                {
                    int idx = (int)(-1 - reqId);
                    hyperclient_attribute ha = HyperClient.get_attr(attrs,idx);
                    throw new HyperClientException(status,ha.getAttrName());
                }
                else
                {
                    throw new HyperClientException(status);
                }
	        }
	
	        client.ops.put(reqId,this);
        }
        finally
        {
            if ( attrs != null ) HyperClient.destroy_attrs(attrs,attrs_sz);
            hyperclient.delete_int_ptr(rc_int_ptr);
        }
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();
        if (status == ReturnCode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else
        {
            throw new HyperClientException(status);
        }
    }
}
