package hyperclient;

import java.util.*;

public class Search extends Pending
{
    SWIGTYPE_p_p_hyperclient_attribute attrs_ptr = null;
    SWIGTYPE_p_size_t attrs_sz_ptr = null;

    private Vector<Object> backlogged = new Vector<Object>();

    public Search(HyperClient client, String space, Map predicate)
                                                    throws HyperClientException,
                                                           TypeError,
                                                           ValueError,
                                                           MemoryError
    {
        super(client);

        if ( predicate == null )
            throw new ValueError("Search critera cannot be null");

        attrs_ptr = hyperclient.new_hyperclient_attribute_ptr();
        attrs_sz_ptr = hyperclient.new_size_t_ptr();

        hyperclient_attribute eq = null;
        int eq_sz = 0;

        hyperclient_range_query rn = null;
        int rn_sz = 0;

        try
        {
            HashMap<String,Object> equalities = new HashMap<String,Object>();
            HashMap<String,Object> ranges = new HashMap<String,Object>();

            for (Iterator it=predicate.keySet().iterator(); it.hasNext();)
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
                    equalities.put(key,val);
                }
                else
                {
                    if ( val instanceof Map.Entry )
                    {
                        try
                        {
                            long lower = ((Long)((Map.Entry)val).getKey()).longValue();
                            long upper = ((Long)((Map.Entry)val).getValue()).longValue();
                        }
                        catch(Exception e)
                        {
                            throw
                                new TypeError(String.format(errStr,val.getClass().getName()));
                        }
                    }
                    else if ( val instanceof List )
                    {
                        try
                        {
                            List listVal = (List)val;
    
                            if ( listVal.size() != 2 )
                                throw new TypeError("Attribute '" + key + "': using a List to specify a range requires its size to be 2, but got size " + listVal.size());  
                        }
                        catch (TypeError te)
                        {
                            throw te;
                        }
                        catch (Exception e)
                        {
                            throw
                                new TypeError(
                                    String.format(errStr,val.getClass().getName()));
                        }
                    }
                    else
                    {
                        throw
                            new TypeError(String.format(errStr,val.getClass().getName()));
                    }

                    ranges.put(key,val);
                }
            }

            if ( equalities.size() > 0 )
            {
                eq_sz = equalities.size();
                eq = HyperClient.dict_to_attrs(equalities);
            }

            if ( ranges.size() > 0 )
            {
                rn_sz = ranges.size();
                rn = HyperClient.alloc_range_queries(rn_sz);
                if ( rn == null ) throw new MemoryError();

                int i = 0;
                for (Iterator it=ranges.keySet().iterator(); it.hasNext();)
                {
                    String key = (String)(it.next());
                    Object val = ranges.get(key);

                    long lower = 0;
                    long upper = 0;

                    if ( val instanceof Map.Entry )
                    {
                        lower = (Long)(((Map.Entry)val).getKey());
                        upper = (Long)(((Map.Entry)val).getValue());
                    }
                    else // Must be a List of Longs of size = 2
                    {
                        lower = (Long)(((List)val).get(0));
                        upper = (Long)(((List)val).get(1));
                    }
    
                    hyperclient_range_query rq = HyperClient.get_range_query(rn,i);

                    if ( HyperClient.write_range_query(rq,key.getBytes(),lower,upper) == 0 )
                        throw new MemoryError();

                    i++;
                }
            }

            if ( eq == null && rn == null )
                        throw new ValueError("Search criteria can't be empty");

            reqId = hyperclient.hyperclient_search(client.get_hyperclient(),
                                                   space,
                                                   eq, eq_sz,
                                                   rn, rn_sz,
                                                   rc_ptr,
                                                   attrs_ptr, attrs_sz_ptr);

	
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
	
            client.ops.put(reqId,this);
        }
        finally
        {
            if ( eq != null ) HyperClient.destroy_attrs(eq, eq_sz);
            if ( rn != null ) HyperClient.destroy_range_queries(rn, rn_sz);
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
            System.out.println("next(): status = " + status());
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
            System.out.println("hasNext(): status = " + status());
        }

        return backlogged.size() > 0;
    }

    protected void finalize() throws Throwable
    {
        super.finalize();

        hyperclient.delete_hyperclient_attribute_ptr(attrs_ptr);
        hyperclient.delete_size_t_ptr(attrs_sz_ptr);
    }
}
