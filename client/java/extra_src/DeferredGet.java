package hyperclient;

import java.util.*;

public class DeferredGet extends Deferred
{
    private SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    private SWIGTYPE_p_size_t attrs_sz_ptr = null;

    public DeferredGet(HyperClient client, Object space, Object key)
                                                    throws HyperClientException,
                                                           TypeError
    {
        super(client);

        attrs_ptr = hyperclient_lc.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient_lc.new_size_t_ptr();

        reqId = client.get(client.getBytes(space,true),
                           client.getBytes(key),
                           rc_ptr,
                           attrs_ptr, attrs_sz_ptr);

        checkReqId(reqId, status());

        client.ops.put(reqId,this);
    }

    public Object waitFor() throws HyperClientException, ValueError
    {
        super.waitFor();

        if (status() == hyperclient_returncode.HYPERCLIENT_SUCCESS)
        {
            hyperclient_attribute attrs
                = hyperclient_lc.hyperclient_attribute_ptr_value(attrs_ptr);
            long attrs_sz = hyperclient_lc.size_t_ptr_value(attrs_sz_ptr);

            Map map = null;

            try
            {
                map = client.attrs_to_dict(attrs, attrs_sz);
            }
            finally
            {
                if ( attrs != null )
                    hyperclient_lc.hyperclient_destroy_attrs(attrs,attrs_sz);
            }

            return map;
        }
        else if (status() == hyperclient_returncode.HYPERCLIENT_NOTFOUND)
        {
            return null;
        }
        else
        {
            throw new HyperClientException(status());
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if (attrs_ptr != null) hyperclient_lc.delete_hyperclient_attribute_ptr(attrs_ptr);
        if (attrs_sz_ptr != null) hyperclient_lc.delete_size_t_ptr(attrs_sz_ptr);
    }
}
