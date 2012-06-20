package hyperclient;

import java.util.*;

abstract class MapOp
{
    protected HyperClient client;

    public MapOp(HyperClient client)
    {
        this.client = client;
    }

    abstract long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr);
}

class MapOpAdd extends MapOp
{
    public MapOpAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_add(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpRemove extends MapOp
{
    public MapOpRemove(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_remove(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicAdd extends MapOp
{
    public MapOpAtomicAdd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_add(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicSub extends MapOp
{
    public MapOpAtomicSub(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_sub(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicMul extends MapOp
{
    public MapOpAtomicMul(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_mul(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicDiv extends MapOp
{
    public MapOpAtomicDiv(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_div(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicMod extends MapOp
{
    public MapOpAtomicMod(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_mod(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicAnd extends MapOp
{
    public MapOpAtomicAnd(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_and(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicOr extends MapOp
{
    public MapOpAtomicOr(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_or(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpAtomicXor extends MapOp
{
    public MapOpAtomicXor(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_atomic_xor(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpStringPrepend extends MapOp
{
    public MapOpStringPrepend(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_string_prepend(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}

class MapOpStringAppend extends MapOp
{
    public MapOpStringAppend(HyperClient client)
    {
        super(client);
    }

    long call(String space, String key,
              hyperclient_map_attribute attrs, long attrs_sz,
              SWIGTYPE_p_hyperclient_returncode rc_ptr)
    {
        return client.map_string_append(space,
                               key.getBytes(),
                               attrs, attrs_sz,
                               rc_ptr);
    }
}
