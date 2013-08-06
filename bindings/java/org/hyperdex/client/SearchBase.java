package org.hyperdex.client;

import java.util.*;

public class SearchBase extends Pending
{
    protected SWIGTYPE_p_p_hyperdex_client_attribute attrs_ptr = null;
    protected SWIGTYPE_p_size_t attrs_sz_ptr = null;

    protected hyperdex_client_attribute_check chks = null;
    protected long chks_sz = 0;

    private Vector<Object> backlogged = new Vector<Object>();

    public SearchBase(Client client)
    {
        super(client);

        attrs_ptr = hyperdex_client.new_hyperdex_client_attribute_ptr();
        attrs_sz_ptr = hyperdex_client.new_size_t_ptr();

    }

    public void callback()
    {
        if ( status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SEARCHDONE ) 
        {
            finished = true;
            client.ops.remove(reqId);
        }
        else if ( status() == hyperdex_client_returncode.HYPERDEX_CLIENT_SUCCESS )
        {
            Map attrsMap = null;

            hyperdex_client_attribute attrs = null;
            long attrs_sz = 0;

            attrs = hyperdex_client.hyperdex_client_attribute_ptr_value(attrs_ptr);

            attrs_sz = hyperdex_client.size_t_ptr_value(attrs_sz_ptr);
            try
            {
                attrsMap =  client.attrs_to_dict(attrs,attrs_sz);
            }
            catch(Exception e) // There's a bug in attrs_to_dict() if exception here. 
            {
                e.printStackTrace();
            }
            finally
            {
                if ( attrs != null )
                {
                    hyperdex_client.hyperdex_client_destroy_attrs(attrs,attrs_sz);
                }
            }

            if ( backlogged.size() < Integer.MAX_VALUE )
            {
                backlogged.add(attrsMap);
            }
            else
            {
                backlogged.set(Integer.MAX_VALUE-1,new HyperDexClientException(
                                             hyperdex_client_returncode.HYPERDEX_CLIENT_NOMEM));
            }
        }
        else
        {
             backlogged.add(new HyperDexClientException(status()));
        }
    }

    public Map next() throws HyperDexClientException
    {
        while ( ! finished && backlogged.size() == 0 )
        {
            client.loop();
        }

        if ( backlogged.size() > 0 )
        {
            Object ret = backlogged.remove(backlogged.size()-1);

            if ( ret instanceof HyperDexClientException )
                throw (HyperDexClientException)ret;
            else
                return (Map)ret;
        }
        else
        {
            return null;
        }
    }

    public boolean hasNext() throws HyperDexClientException
    {
        if ( backlogged.size() > 0 ) return true;

        while ( ! finished && backlogged.size() == 0 )
        {
            client.loop();
        }

        return backlogged.size() > 0;
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        if (attrs_ptr != null) hyperdex_client.delete_hyperdex_client_attribute_ptr(attrs_ptr);
        if (attrs_sz_ptr != null) hyperdex_client.delete_size_t_ptr(attrs_sz_ptr);
    }
}
