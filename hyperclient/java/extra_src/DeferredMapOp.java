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
	      int attrs_sz = 0;
	
        try
        {
	        attrs = HyperClient.dict_to_map_attrs(map);
	        attrs_sz = 6;
	
	        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();
	
	        reqId = op.call(space, key, attrs, attrs_sz, rc_int_ptr);
	
	        if (reqId < 0)
	        {
	            status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));

                if ( status == ReturnCode.HYPERCLIENT_UNKNOWNATTR
                  || status == ReturnCode.HYPERCLIENT_DUPEATTR // Won't happen
                  || status == ReturnCode.HYPERCLIENT_WRONGTYPE)
                {
                    long idx = BigInteger.ONE.subtract(
                                BigInteger.valueOf(reqId)).longValue();

                    hyperclient_map_attribute hma = HyperClient.get_map_attr(attrs,idx);
                    throw new HyperClientException(status,hma.getMapAttrName());
                }
                else
                {
                    throw new HyperClientException(status);
                }
	        }
	
	        client.ops.put(reqId,this);
        }
        catch(HyperClientException he)
        {
            throw he;
        }
        catch(TypeError te)
        {
            throw te;
        }
        catch(MemoryError me)
        {
            throw me;
        }
        finally
        {
            if ( attrs != null ) HyperClient.destroy_map_attrs(attrs,attrs_sz);
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
