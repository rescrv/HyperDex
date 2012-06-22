
class MapOp__CAMEL_NAME__ extends MapOp
{
    public MapOp__CAMEL_NAME__(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.__NAME__(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
