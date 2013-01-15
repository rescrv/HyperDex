package hyperclient;

import java.util.*;

class Predicate
{
    protected List<Map.Entry<hyperpredicate,Object>> raw;

    List<Map.Entry<ByteArray,Map.Entry<hyperpredicate,Object>>>
                                                    getRawChecks(ByteArray attribute)
    {
        if ( raw = null )
        {
            return null;
        }
        else
        {
            Vector<Map.Entry<Object,Map.Entry<hyperpredicate,Object>>> retval
                = new Vector<Map.Entry<Object,Map.Entry<hyperpredicate,Object>>>();

            for ( Map.Entry<hyperpredicate,Object> entry: raw )
            {
                retval.add(new Map.Entry<Object,Map.Entry<hyperpredicate,Object>>(
                    attribute,new Map.Entry<hyperpredicate,Object>(
                        raw.getKey(),raw.getValue())));
            }

            return retval;
        }
    }
}
