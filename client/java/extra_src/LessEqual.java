package hyperclient;

import java.util.*;

public class LessEqual extends Predicate
{
    public LessEqual(Object upper) throws AttributeError
    {
        if (   ! HyperClient.isBytes(upper)
            && ! (upper instanceof Long)   
            && ! (upper instanceof Double) )
        {
            throw new AttributeError("LessEqual must be a byte[], ByteArray, String, Long, or Double");
        }  

        List<Map.Entry<hyperpredicate,Object>> raw
            = new Vector<Map.Entry<hyperpredicate,Object>>(1);

        raw.add(new AbstractMap.SimpleEntry<hyperpredicate,Object>(
                hyperpredicate.HYPERPREDICATE_LESS_EQUAL,upper));

        this.raw = raw;
    }
}

