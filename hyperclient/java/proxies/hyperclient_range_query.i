// Write our own (safe) setter for attr
// in order to avoid warnings
//
%ignore hyperclient_range_query::attr;

%typemap(javacode) hyperclient_range_query
%{
  String getRangeQueryAttrName()
  {
    return HyperClient.read_range_query_attr_name(this);
  }
%}
