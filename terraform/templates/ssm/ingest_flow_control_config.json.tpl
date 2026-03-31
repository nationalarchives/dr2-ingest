{
  "maxConcurrency": "${max_concurrency}",
  "enabled": true,
  "sourceSystems": [
    {
      "systemName": "TDR",
      "reservedChannels": "${tdr_reserved_channels}",
      "probability": 50
    },
    {
      "systemName": "COURTDOC",
      "reservedChannels": "${courtdoc_reserved_channels}",
      "probability": 30
    },
    {
      "systemName": "DEFAULT",
      "reservedChannels": 0,
      "probability": 20
    }
  ]
}