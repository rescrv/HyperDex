package hyperclient;

import java.util.*;

public class DeferredCondPut extends Deferred
{
    public DeferredCondPut(HyperClient client, String space, String key,
                                                            Map condition, Map value)
                                                    throws HyperClientException,
                                                           TypeError,
                                                           MemoryError
    {
        super(client);

        hyperclient_attribute condattrs = null;
        long condattrs_sz = 0;

        hyperclient_attribute attrs = null;
        long attrs_sz = 0;

        try
        {
            condattrs = HyperClient.dict_to_attrs(condition);
            condattrs_sz = condition.size();

            attrs = HyperClient.dict_to_attrs(value);
            attrs_sz = value.size();
	
            reqId = hyperclient.hyperclient_condput(client.get_hyperclient(),
                                                    space,
                                                    key.getBytes(),
                                                    condattrs, condattrs_sz,
                                                    attrs, attrs_sz,
                                                    rc_ptr);
	        if (reqId < 0)
	        {
                int idx = (int)(-1 - reqId);
                String attrName = null;

                if ( condattrs != null && idx >= 0 && idx < condattrs_sz )
                    attrName = HyperClient.get_attr(condattrs,idx).getAttrName(); 

                idx -= condattrs_sz;

                if ( attrs != null && idx >= 0 && idx < attrs_sz )
                    attrName = HyperClient.get_attr(attrs,idx).getAttrName(); 

                if ( attrName != null )
                {
                    throw new HyperClientException(status(),attrName);
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
            if ( condattrs != null ) HyperClient.destroy_attrs(condattrs,condattrs_sz);
            if ( attrs != null ) HyperClient.destroy_attrs(attrs,attrs_sz);
        }
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();

        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            return new Boolean(true);
        }
        else if (status() == hyperclient_returncode.HYPERCLIENT_CMPFAIL)
        {
            return new Boolean(false);
        }
        else
        {
            throw new HyperClientException(status());
        }
    }
}
