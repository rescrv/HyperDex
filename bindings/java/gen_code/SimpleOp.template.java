
class SimpleOp__CAMEL_NAME__ extends SimpleOp
{
    public SimpleOp__CAMEL_NAME__(Client client)
    {
        super(client);
    }

    long call(Object space, Object key,
              hyperdex_client_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperdex_client_returncode rc_ptr) throws TypeError
    {
        return client.__NAME__(client.getBytes(space,true),
                               client.getBytes(key),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
