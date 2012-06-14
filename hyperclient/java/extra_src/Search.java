package hyperclient;

import java.util.*;

public class Search
{
    private HyperClient client = null;
    protected long reqId = 0; 
    ReturnCode status = ReturnCode.HYPERCLIENT_ZERO; // Give this package wide access
    protected boolean finished = false; 

    SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    SWIGTYPE_p_size_t attrs_sz_ptr = null;

    public Search(HyperClient client, String space, Map predicate)
                                                    throws HyperClientException,
                                                           MemoryError
    {
        this.client = client;

        attrs_ptr = hyperclient.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

        try
        {
            //reqId = client.get(space, key, rc_int_ptr, attrs_ptr, attrs_sz_ptr);
	
            if (reqId < 0)
            {
                status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));
                throw new HyperClientException(status);
            }
	
            //client.ops.put(reqId,this);
        }
        finally
        {
            hyperclient.delete_int_ptr(rc_int_ptr);
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

        hyperclient.delete_hyperclient_attribute_ptr(attrs_ptr);
        hyperclient.delete_size_t_ptr(attrs_sz_ptr);
    }
}
