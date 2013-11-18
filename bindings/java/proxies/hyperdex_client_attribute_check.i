// Write our own (safe) getters and setters for attr and value
// in order to avoid warnings
//
%ignore hyperdex_client_attribute_check::attr;
%ignore hyperdex_client_attribute_check::value;

%typemap(javacode) hyperdex_client_attribute_check
%{
  byte[] getAttrNameBytes()
  {
    int name_sz = Client.get_attr_check_name_sz(this);
    byte[] bytes = new byte[name_sz];

    Client.read_attr_check_name(this,bytes);

    return bytes;
  }
%}
