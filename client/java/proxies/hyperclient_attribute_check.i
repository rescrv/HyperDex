// Write our own (safe) getters and setters for attr and value
// in order to avoid warnings
//
%ignore hyperclient_attribute_check::attr;
%ignore hyperclient_attribute_check::value;

%typemap(javacode) hyperclient_attribute_check
%{
  byte[] getAttrNameBytes()
  {
    int name_sz = HyperClient.get_attr_check_name_sz(this);
    byte[] bytes = new byte[name_sz];

    HyperClient.read_attr_check_name(this,bytes);

    return bytes;
  }
%}
