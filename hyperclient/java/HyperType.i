// Write our own (safe) getters and setters for attr and value
// in order to avoid warnings
//
%ignore hyperclient_attribute::attr;
%ignore hyperclient_attribute::value;

%typemap(javacode) hyperclient_attribute
%{
  public String getAttrName()
  {
    return HyperClient.read_attr_name(this);
  }

  private byte[] getValueBytes()
  {
    long len = HyperClient.read_attr_value(this,new byte[0]);

    int bytes_sz = HyperClient.size_t_to_int(len);

    byte[] bytes = new byte[bytes_sz];
    HyperClient.read_attr_value(this,bytes);
    return bytes;
  }

  public java.lang.Object getStringValue()
  {
    return new String(getValueBytes());
  }

  private java.lang.Object getLongValue()
  {
    return
      new Long(
        java.nio.ByteBuffer.wrap(getValueBytes()).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong());
  }

  public java.lang.Object getValue()
  {
    switch(getDatatype())
    {
      case HYPERDATATYPE_STRING:
        return getStringValue();

      case HYPERDATATYPE_INT64:
        return getLongValue();

      default:
        return null;
    }
  }
%}

%typemap(javacode) HyperClient
%{
  java.util.HashMap<Long,Deferred> ops = new java.util.HashMap<Long,Deferred>(); 

  void loop() throws HyperClientException
  {
    SWIGTYPE_p_int rc_int_ptr = hyperclient.new_int_ptr();

    long ret = loop(rc_int_ptr);

    ReturnCode rc = ReturnCode.swigToEnum(hyperclient.int_ptr_value(rc_int_ptr));

    if ( ret < 0 )
    {
      throw new HyperClientException(rc);
    }
    else
    {
      Deferred d = ops.get(ret);
      d.status = rc;
      d.callback();
    }
  }

  // Deal's with Java's int size limit for objects.
  // Return the int closest to size_t representation
  // Ideally, in a future java version, this method will
  // return the same value ie., do nothing or simply be removed.
  //
  static int size_t_to_int(long l)
  {
    int i = 0;

    if ( l < 0 || l > Integer.MAX_VALUE )
    {
        i = Integer.MAX_VALUE;
    }
    else
    {
        i = (int)l;
    }

    return i;
  }

  static java.util.Map attrs_to_map(hyperclient_attribute attrs, long attrs_sz)
  {
    java.util.HashMap<Object,Object> map = new java.util.HashMap<Object,Object>();

    int sz = HyperClient.size_t_to_int(attrs_sz);

    for ( int i=0; i<sz; i++)
    {
        hyperclient_attribute ha = get_attr(attrs,i);
        map.put(ha.getAttrName(),ha.getValue());
    }

    return map;
  }

  static hyperclient_attribute map_to_attrs(java.util.Map map) throws TypeError,
                                                                        MemoryError
  {
    int attrs_sz = map.size();
    hyperclient_attribute attrs = alloc_attrs(attrs_sz);

    if ( attrs == null ) throw new MemoryError();
    
    int i = 0;

    for (java.util.Iterator it=map.keySet().iterator(); it.hasNext();)
    {
        hyperclient_attribute ha = get_attr(attrs,i);

        String attrStr = (String)(it.next());
        byte[] attrBytes = attrStr.getBytes();

        Object value = map.get(attrStr);

        try
        {
            hyperdatatype type;

            if ( value instanceof String )
            {
                type = hyperdatatype.HYPERDATATYPE_STRING;
                byte[] valueBytes = ((String)value).getBytes();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
    
            }
            else if ( value instanceof Long )
            {
                type = hyperdatatype.HYPERDATATYPE_INT64;
                byte[] valueBytes = java.nio.ByteBuffer.allocate(8).order(
                    java.nio.ByteOrder.LITTLE_ENDIAN).putLong(
                        ((Long)value).longValue()).array();
                if (write_attr_value(ha, valueBytes) == 0) throw new MemoryError();
            }
            else
            {
                throw
                    new TypeError("Unsupported type: " + value.getClass().getName());
            }

            if (write_attr_name(ha, attrBytes, type) == 0) throw new MemoryError();
        }
        catch(MemoryError me)
        {
            destroy_attrs(attrs, attrs_sz);
            throw me;
        }
        catch(TypeError te)
        {
            destroy_attrs(attrs, attrs_sz);
            throw te;
        }

        i++;
    }

    return attrs;
  }

  public Deferred async_get(String space, String key) throws HyperClientException
  {
    return new DeferredGet(this,space, key);
  }

  public java.util.Map get(String space, String key) throws HyperClientException
  {
    DeferredGet d = (DeferredGet)(async_get(space, key));
    return (java.util.Map)(d.waitFor());
  }

  public Deferred async_put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError
  {
    return new DeferredFromAttrs(this, new SimpleOpPut(this), space, key, map);
  }

  public boolean put(String space, String key, java.util.Map map)
                                                     throws HyperClientException,
                                                            TypeError,
                                                            MemoryError
  {
    Deferred d = (DeferredFromAttrs)(async_put(space, key, map));
    return ((Boolean)(d.waitFor())).booleanValue();
  }
%}
