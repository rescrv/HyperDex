// Write our own (safe) getters for attr and setters for attr, map_key and value
// in order to avoid warnings
//
%ignore hyperclient_map_attribute::attr;
%ignore hyperclient_map_attribute::map_key;
%ignore hyperclient_map_attribute::value;

%typemap(javacode) hyperclient_map_attribute
%{
  public String getMapAttrName()
  {
    return HyperClient.read_map_attr_name(this);
  }
%}
