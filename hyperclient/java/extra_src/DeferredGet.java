package hyperclient;

import java.util.*;

public class DeferredGet extends Deferred
{
    SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    SWIGTYPE_p_size_t attrs_sz_ptr = null;

    public DeferredGet(HyperClient client, String space, String key)
                                                    throws HyperClientException
    {
        super(client);

        attrs_ptr = hyperclient.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

        reqId = client.get(space, key, rc_int_ptr, attrs_ptr, attrs_sz_ptr);

        if (reqId < 0)
        {
            status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));
            throw new HyperClientException(status);
        }


        client.ops.put(reqId,this);
    }

    public Object waitFor() throws HyperClientException
    {
        super.waitFor();
        if (status == ReturnCode.HYPERCLIENT_SUCCESS)
        {
            hyperclient_attribute attrs
                = hyperclient.hyperclient_attribute_ptr_value(attrs_ptr);
            long attrs_sz = hyperclient.size_t_ptr_value(attrs_sz_ptr);

            Map map = HyperClient.attrs_to_map(attrs, attrs_sz);
            
            hyperclient.hyperclient_destroy_attrs(attrs,attrs_sz);

            attrs_ptr = null;
            attrs_sz_ptr = null;

            return map;
        }
        else if (status == ReturnCode.HYPERCLIENT_NOTFOUND)
        {
            return null;
        }
        else
        {
            throw new HyperClientException(status);
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if ( attrs_ptr != null ) 
        {
            hyperclient_attribute attrs
                = hyperclient.hyperclient_attribute_ptr_value(attrs_ptr);
            long attrs_sz = hyperclient.size_t_ptr_value(attrs_sz_ptr);

            hyperclient.hyperclient_destroy_attrs(attrs,attrs_sz);
        }
    }
}
