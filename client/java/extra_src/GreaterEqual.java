public class GreaterEqual extends Predicate
{
    public GreaterEqual(Object lower) throws AttributeError
    {
        if (   ! HyperClient.isBytes(lower)
            && ! lower instanceof Long   
            && ! lower instanceof Double )
        {
            throw new AttributeError("GreaterEqual must be a byte[], ByteArray, String, Long, or Double")
        }  

        List<Map.Entry<hyperpredicate,Object>> raw
            = new Vector<Map.Entry<hyperpredicate,Object>>(1);

        raw.put(hyperpredicate.HYPERPREDICATE_GREATER_EQUAL,lower);

        this.raw = raw;
    }
}
