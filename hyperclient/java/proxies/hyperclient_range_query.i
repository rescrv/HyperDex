// Write our own (safe) setter for attr
// in order to avoid warnings
//
%ignore hyperclient_range_query::attr;

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
