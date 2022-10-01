var source = mongodb({
  "uri": "",
  "timeout": "30s",
  "tail": true,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"],
  // "wc": 1,
  // "fsync": false,
  // "bulk":true,
  // "collection_filters": "{}",
  "read_preference": "Primary"
})

var sink = mongodb({
  "uri": "",
  "timeout": "30s",
   "tail": true,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"],
  // "wc": 1,
  // "fsync": false,
  "bulk": true,
    "set":true,
  // "collection_filters": "{}",
  // "read_preference": "Primary"
})

t.Source("source", source, "/^(nodeStaticInfo|nodeInfo)$/")
    .Transform(remap({"ns_map":{
        "nodeStaticInfo":  "joinNodes",
            "nodeInfo":  "joinNodes"
        }}))
    .Save("sink", sink, "/.*/")
