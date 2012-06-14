package hyperclient;

import java.util.*;

public class Search extends Pending
{
    SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    SWIGTYPE_p_size_t attrs_sz_ptr = null;

    public Search(HyperClient client, String space, Map predicate)
                                                    throws HyperClientException,
                                                           TypeError,
                                                           MemoryError
    {
        super(client);

        attrs_ptr = hyperclient.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

        SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

        try
        {
            HashMap<String,Object> equalities = new HashMap<String,Object>
            HashMap<String,Object> ranges = new HashMap<String,Object>

            for (Iterator it=predicate.keySet().iterator(); iterator.hasNext();)
            {
                String key = (String)(it.next());

                if ( key == null )
                    throw new TypeError("Cannot search on a null attribute");

                Object val = predicate.get(key);

                if ( val == null )
                    throw new TypeError("Cannot search with a null criteria");


                String errStr = "Attribute '" + key + "' has incorrect type ( expected Long, String, Map.Entry<Long,Long> or List<Long> (of size 2), but got %s";

                if ( val instanceof String || val instanceof Long)
                {
                    equalities.put(key,val)
                }
                else
                {
                    if ( val instanceof Map.Entry )
                    {
                        try
                        {
                            long lower = ((Map.Entry<Long,Long>)val).getKey().valueOf();
                            long upper = ((Map.Entry<Long,Long>)val).getValue().valueOf();
                        }
                        catch(Exception)
                        {
                            throw
                                new TypeError(String.format(errStr,val.getClass().getName());
                        }
                    }
                    else if ( val instanceof List )
                    {
                        try
                        {
                            List<Long> listVal = (List<Long>)val;
    
                            if ( listVal.size() != 2 )
                                throw new TypeError("Attribute '" + key + "': using a List to specify a range requires its size to be 2, but got size " + listVale.size());  
                        }
                        catch (TypeError te)
                        {
                            throw te;
                        }
                        catch (Exception e)
                        {
                            throw
                                new TypeError(
                                    String.format(errStr,val.getClass().getName());
                        }
                    }
                    else
                    {
                        throw
                            new TypeError(String.format(errStr,val.getClass().getName());
                    }

                    ranges.put(key,val);
                }
            }

            //reqId = client.get(space, key, rc_int_ptr, attrs_ptr, attrs_sz_ptr);
	
            if (reqId < 0)
            {
                status = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));
                throw new HyperClientException(status);
            }
	
            client.ops.put(reqId,this);
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
