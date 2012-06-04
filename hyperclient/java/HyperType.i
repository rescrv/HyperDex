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
    int len = HyperClient.read_value(this,new byte[0]);
    byte[] bytes = new byte[len];
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
  public java.util.Map get(String space, String key)
  {
    java.util.HashMap<Object,Object> map = new java.util.HashMap<Object,Object>();

    SWIGTYPE_p_p_hyperclient_attribute pattrs_ptr = hyperclient.new_phyperclient_attribute_ptr();
    SWIGTYPE_p_int attrs_sz_ptr = hyperclient.new_int_ptr();

    ReturnCode ret = get(space,key,pattrs_ptr,attrs_sz_ptr);

    hyperclient_attribute attrs
        = hyperclient.phyperclient_attribute_ptr_value(pattrs_ptr);
    int attrs_sz = hyperclient.int_ptr_value(attrs_sz_ptr);

    for ( int i=0; i<attrs_sz; i++)
    {
        hyperclient_attribute ha = get_attribute(attrs,i);
        map.put(ha.getAttr(),ha.getValue());
    }

    // XXX Don't forget to implement hyperclient_attribute_destroy and call it here

    return map;
  }
%}
