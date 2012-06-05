// Write our own (safe) getters and setters for attr and value
// in order to avoid warnings
//
%ignore hyperclient_attribute::attr;
%ignore hyperclient_attribute::value;

%typemap(javacode) hyperclient_attribute
%{
  public String getAttr()
  {
    return HyperClient.read_attr_name(this);
  }

  private byte[] getValueBytes()
  {
    long len = HyperClient.read_value(this,new byte[0]);

    int bytes_sz = HyperClient.size_t_to_int(len);

    byte[] bytes = new byte[bytes_sz];
    HyperClient.read_value(this,bytes);
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
        hyperclient_attribute ha = get_attribute(attrs,i);
        map.put(ha.getAttr(),ha.getValue());
    }

    return map;
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

  /*
  public java.util.Map get(String space, String key)
  {
    java.util.HashMap<Object,Object> map = new java.util.HashMap<Object,Object>();

    SWIGTYPE_p_p_hyperclient_attribute pattrs_ptr = hyperclient.new_phyperclient_attribute_ptr();
    SWIGTYPE_p_size_t attrs_sz_ptr = hyperclient.new_size_t_ptr();

    ReturnCode ret = get(space,key,pattrs_ptr,attrs_sz_ptr);

    hyperclient_attribute attrs
        = hyperclient.phyperclient_attribute_ptr_value(pattrs_ptr);
    long attrs_sz = hyperclient.size_t_ptr_value(attrs_sz_ptr);

    int sz = HyperClient.size_t_to_int(attrs_sz);

    for ( int i=0; i<sz; i++)
    {
        hyperclient_attribute ha = get_attribute(attrs,i);
        map.put(ha.getAttr(),ha.getValue());
    }

    hyperclient.hyperclient_destroy_attrs(attrs,attrs_sz);

    return map;
  }
  */
%}
