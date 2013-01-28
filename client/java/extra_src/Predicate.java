package hyperclient;

import java.util.*;

public abstract class Predicate
{
    protected List<Map.Entry<hyperpredicate,Object>> raw;

    List<Map.Entry<ByteArray,Map.Entry<hyperpredicate,Object>>>
                                                    getRawChecks(ByteArray attribute)
    {
        if ( raw == null )
        {
            return null;
        }
        else
        {
            Vector<Map.Entry<ByteArray,Map.Entry<hyperpredicate,Object>>> retval
                = new Vector<Map.Entry<ByteArray,Map.Entry<hyperpredicate,Object>>>();

            for ( Map.Entry<hyperpredicate,Object> entry: raw )
            {
                retval.add(new AbstractMap.SimpleEntry<
                    ByteArray,Map.Entry<hyperpredicate,Object>>(
                        attribute, new AbstractMap.SimpleEntry<hyperpredicate,Object>(
                            entry.getKey(),entry.getValue())));
            }

            return retval;
        }
    }
}
