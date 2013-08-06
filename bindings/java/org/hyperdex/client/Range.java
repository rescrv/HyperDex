package org.hyperdex.client;

import java.util.*;

public class Range extends Predicate
{
    public Range(Object lower, Object upper) throws AttributeError
    {
        if (   ! ( Client.isBytes(lower) && Client.isBytes(upper) )
            && ! ( lower instanceof Long && upper instanceof Long )  
            && ! ( lower instanceof Double && upper instanceof Double ) )
        {
            throw new AttributeError("Range search bounds must be of like types");
        }  

        List<Map.Entry<hyperpredicate,Object>> raw
            = new Vector<Map.Entry<hyperpredicate,Object>>(2);

        raw.add(new AbstractMap.SimpleEntry<hyperpredicate,Object>(
                    hyperpredicate.HYPERPREDICATE_GREATER_EQUAL,lower));
        raw.add(new AbstractMap.SimpleEntry<hyperpredicate,Object>(
                    hyperpredicate.HYPERPREDICATE_LESS_EQUAL,upper));

        this.raw = raw;
    }
}
