#! /bin/bash

start_mongo() {
    mkdir -p /tapdata/data/logs
    mongod --dbpath=/tapdata/data/db/ --replSet=rs0 --wiredTigerCacheSizeGB=1 --bind_ip_all --logpath=/tapdata/data/logs/mongod.log --fork
    while [[ 1 ]]; do
        mongo --quiet --eval "db" &> /dev/null
        if [[ $? -eq 0 ]]; then
            break
        fi
        sleep 1
    done
    mongo --quiet --eval 'rs.initiate({"_id":"rs0","members":[{"_id":0,"host":"127.0.0.1:27017"}]})'
    while [[ 1 ]]; do
        mongo --quiet --eval "rs.status()"|grep PRIMARY &> /dev/null
        if [[ $? -eq 0 ]]; then
            break
        fi
        sleep 1
    done
    while [[ 1 ]]; do
        mongo --quiet --eval 'db.getSiblingDB("tapdata").getCollection("AccessToken").exists()'|grep "NotPrimaryNoSecondaryOk" &> /dev/null
        if [[ $? -ne 0 ]]; then
            break
        fi
        sleep 1
    done
    mongo --quiet --eval 'db.getSiblingDB("tapdata").getCollection("AccessToken").exists()'|grep -v null &> /dev/null
}

start_server() {
    # 1. start manager server
    # 2. register connectors
    # 3. start iengine server
    cd /tapdata/apps/manager/bin && bash start.sh
    cd /tapdata/apps/iengine/bin && bash start.sh
}

unzip_files() {
    tar xzf /tapdata/apps/connectors/dist.tar.gz -C /tapdata/apps/
}

_main() {
    unzip_files
    start_mongo
    start_server
    sleep infinity
}

_main "$@"
