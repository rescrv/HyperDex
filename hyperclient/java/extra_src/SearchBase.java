package hyperclient;

import java.util.*;

public class SearchBase extends Pending
{
    protected SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    protected SWIGTYPE_p_size_t attrs_sz_ptr = null;

    protected hyperclient_attribute eq = null;
    protected int eq_sz = 0;

    protected hyperclient_range_query rn = null;
    protected int rn_sz = 0;

    private Vector<Object> backlogged = new Vector<Object>();

    public SearchBase(HyperClient client)
    {
        super(client);

        attrs_ptr = hyperclient.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

    }

    protected void checkReqId(hyperclient_attribute eq, int eq_sz,
                              hyperclient_range_query rn, int rn_sz)
                                                        throws HyperClientException
    {
        if (reqId < 0)
        {
            int idx = (int)(-1 - reqId);
            String attrName = null;

            if ( idx >= 0 && idx < eq_sz && eq != null )
                attrName = HyperClient.get_attr(eq,idx).getAttrName(); 
        
            idx -= eq_sz;

            if ( idx >= 0 && idx < rn_sz && rn != null )
                attrName
                    = HyperClient.get_range_query(rn,idx).getRangeQueryAttrName();

            if ( attrName != null )
            {
                throw new HyperClientException(status(),attrName);
            }
            else
            {
                throw new HyperClientException(status());
            }
        }
    }

    public void callback()
    {
        if ( status() == hyperclient_returncode.HYPERCLIENT_SEARCHDONE ) 
        {
            finished = true;
            client.ops.remove(reqId);
        }
        else if ( status() == hyperclient_returncode.HYPERCLIENT_SUCCESS )
        {
            Map attrsMap = null;

            hyperclient_attribute attrs = null;
            long attrs_sz = 0;

            attrs = hyperclient.hyperclient_attribute_ptr_value(attrs_ptr);

            attrs_sz = hyperclient.size_t_ptr_value(attrs_sz_ptr);
            try
            {
                attrsMap =  HyperClient.attrs_to_dict(attrs,attrs_sz);
            }
            catch(Exception e) // There's a bug in attrs_to_dict() if exception here. 
            {
                e.printStackTrace();
            }
            finally
            {
                if ( attrs != null )
                {
                    hyperclient.hyperclient_destroy_attrs(attrs,attrs_sz);
                }
            }

            if ( backlogged.size() < Integer.MAX_VALUE )
            {
                backlogged.add(attrsMap);
            }
            else
            {
                backlogged.set(Integer.MAX_VALUE-1,new HyperClientException(
                                             hyperclient_returncode.HYPERCLIENT_NOMEM));
            }
        }
        else
        {
             backlogged.add(new HyperClientException(status()));
        }
    }

    public Map next() throws HyperClientException
    {
        while ( ! finished && backlogged.size() == 0 )
        {
            client.loop();
        }

        if ( backlogged.size() > 0 )
        {
            Object ret = backlogged.remove(backlogged.size()-1);

            if ( ret instanceof HyperClientException )
                throw (HyperClientException)ret;
            else
                return (Map)ret;
        }
        else
        {
            return null;
        }
    }

    public boolean hasNext() throws HyperClientException
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

        if (attrs_ptr != null) hyperclient.delete_hyperclient_attribute_ptr(attrs_ptr);
        if (attrs_sz_ptr != null) hyperclient.delete_size_t_ptr(attrs_sz_ptr);
    }
}
