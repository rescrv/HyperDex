// Write our own (safe) setters for attr, map_key and value
// in order to avoid warnings
//
%ignore hyperclient_map_attribute::attr;
%ignore hyperclient_map_attribute::map_key;
%ignore hyperclient_map_attribute::value;
