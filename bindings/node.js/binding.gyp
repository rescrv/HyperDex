{
  "targets": [{
    "target_name": "hyperdex-client"
  , "sources": [
      "client.cc"
    ]
  , "include_dirs": [
      "../../include"
    ]
  , "link_settings": {
      "libraries": ["-lhyperdex-client"]
      }
  , "sources": [
        "client.cc"
      , "client.declarations.cc"
      , "client.definitions.cc"
      , "client.prototypes.cc"
    ]
  }]
}
