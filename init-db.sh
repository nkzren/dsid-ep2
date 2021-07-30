#!/bin/bash
docker exec -ti dsid-ep2_mongo_1 mongoimport --username=root --password=example --authenticationDatabase admin  --type csv -d dsid -c inventory --headerline --drop /tmp/isd-inventory.csv