// Write our own (safe) getters for attr and setters for attr, map_key and value
// in order to avoid warnings
//
%ignore hyperdex_client_map_attribute::attr;
%ignore hyperdex_client_map_attribute::map_key;
%ignore hyperdex_client_map_attribute::value;

%typemap(javacode) hyperdex_client_map_attribute
%{
  private java.math.BigInteger attrs_sz_bi = java.math.BigInteger.ZERO;

  // Returns the size of the hyperdex_client_map_attribute array that this instance
  // is the head of. This signed 64 bit integer will be interpreted as an
  // unsigned 64 bit integer at the c/c++ level.
  //
  long getAttrsSz()
  {
    return attrs_sz_bi.longValue();
  }

  java.math.BigInteger getAttrsSz_bi()
  {
    return attrs_sz_bi;
  }

  void setAttrsSz_bi(java.math.BigInteger value)
  {
    attrs_sz_bi = value;
  }

  byte[] getMapAttrNameBytes()
  {
    int name_sz = Client.get_map_attr_name_sz(this);
    byte[] bytes = new byte[name_sz];

    Client.read_map_attr_name(this,bytes);

    return bytes;
  }
%}
