
class SimpleOp__CAMEL_NAME__ extends SimpleOp
{
    public SimpleOp__CAMEL_NAME__(HyperClient client)
    {
        super(client);
    }

    long call(byte[] space, byte[] key,
              hyperclient_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.__NAME__(space,
                               key,
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
