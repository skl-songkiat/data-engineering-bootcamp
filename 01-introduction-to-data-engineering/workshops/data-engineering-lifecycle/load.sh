#!/bin/bash

API_KEY='$2b$10$zmebSYoyZx/atPqm2SSbLu4igtJGfxw9JpT8jZDPUywTHRymDWsJi' # x-master-key
COLLECTION_ID='64cdf982b89b1e2299cbae23'

curl -XPOST \
  -H "Content-type: application/json" \
  -H "X-Master-Key: $API_KEY" \
  -H "X-Collection-Id: $COLLECTION_ID" \
  -d @dogs.json \
  "https://api.jsonbin.io/v3/b"