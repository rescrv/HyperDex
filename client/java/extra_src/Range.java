package hyperclient;

public class Range extends Predicate
{
    public Range(Object lower, Object upper) throws AttributeError
    {
        if (   ! ( HyperClient.isBytes(lower) && HyperClient.isBytes(upper) )
            && ! ( lower instanceof Long && upper instanceof Long )  
            && ! ( lower instanceof Double && upper instanceof Double ) )
        {
            throw new AttributeError("Range search bounds must be of like types")
        }  

        List<Map.Entry<hyperpredicate,Object>> raw
            = new Vector<Map.Entry<hyperpredicate,Object>>(2);

        raw.put(hyperpredicate.HYPERPREDICATE_GREATER_EQUAL,lower);
        raw.put(hyperpredicate.HYPERPREDICATE_LESS_EQUAL,upper);

        this.raw = raw;
    }
}
