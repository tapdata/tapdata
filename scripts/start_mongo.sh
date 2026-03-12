#!/bin/sh
mkdir -p /tapdata/data/logs /tapdata/data/db
mongod --dbpath=/tapdata/data/db/ --replSet=rs0 --bind_ip_all --logpath=/tapdata/data/logs/mongod.log