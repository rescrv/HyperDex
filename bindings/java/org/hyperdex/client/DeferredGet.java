package org.hyperdex.client;

import java.util.*;

public class DeferredGet extends Deferred
{
    private SWIGTYPE_p_p_hyperdex_client_attribute attrs_ptr = null;
    private SWIGTYPE_p_size_t attrs_sz_ptr = null;

    public DeferredGet(Client client, Object space, Object key)
                                                    throws HyperDexClientException,
                                                           TypeError
    {
        super(client);

        attrs_ptr = hyperdex_client.new_hyperdex_client_attribute_ptr();
        attrs_sz_ptr = hyperdex_client.new_size_t_ptr();

        reqId = client.get(client.getBytes(space,true),
                           client.getBytes(key),
                           rc_ptr,
                           attrs_ptr, attrs_sz_ptr);

        checkReqId(reqId, status());

        client.ops.put(reqId,this);
    }

    public Object waitFor() throws HyperDexClientException, ValueError
    {
        super.waitFor();

        if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS)
        {
            hyperdex_client_attribute attrs
                = hyperdex_client.hyperdex_client_attribute_ptr_value(attrs_ptr);
            long attrs_sz = hyperdex_client.size_t_ptr_value(attrs_sz_ptr);

            Map map = null;

            try
            {
                map = client.attrs_to_dict(attrs, attrs_sz);
            }
            finally
            {
                if ( attrs != null )
                    hyperdex_client.hyperdex_client_destroy_attrs(attrs,attrs_sz);
            }

            return map;
        }
        else if (status() == hyperdex_client_returncode.HYPERDEX_CLIENT_NOTFOUND)
        {
            return null;
        }
        else
        {
            throw new HyperDexClientException(status());
        }
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if (attrs_ptr != null) hyperdex_client.delete_hyperdex_client_attribute_ptr(attrs_ptr);
        if (attrs_sz_ptr != null) hyperdex_client.delete_size_t_ptr(attrs_sz_ptr);
    }
}
