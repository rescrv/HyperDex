// Write our own (safe) setter for attr
// in order to avoid warnings
//
%ignore hyperclient_range_query::attr;

// No need to expose upper_t and lower_t in java
//
%ignore hyperclient_range_query::upper_t;
%ignore hyperclient_range_query::lower_t;

%typemap(javacode) hyperclient_range_query
%{
  byte[] getRangeQueryAttrNameBytes()
  {
    int name_sz = HyperClient.get_range_query_attr_name_sz(this);
    byte[] bytes = new byte[name_sz];

    HyperClient.read_range_query_attr_name(this,bytes);

    return bytes;
  }
%}
