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
    if [[ "x"$tapdata_run_env != "xlocal" ]]; then
        if [[ "x"$mode == "xuse" ]]; then
            cd /tapdata/apps
            cd manager && bash bin/start.sh && cd -
            cd iengine && bash bin/start.sh && cd -
        fi

        if [[ "x"$mode == "xdev" ]]; then
            cd /tapdata-source
            cd manager/dist && bash bin/start.sh && cd -
            cd iengine/dist && bash bin/start.sh && cd -
        fi

        if [[ "x"$mode == "xtest" ]]; then
            cd /tapdata/apps
            cd manager && bash bin/start.sh && cd -
            cd iengine && bash bin/start.sh && cd -
        fi
    fi

    if [[ "x"$mode == "xuse" ]]; then
        cd /tapdata/apps/tapshell
        bash register-all-connectors.sh
        echo "register_connector_complete !"
    fi

    if [[ "x"$mode == "xtest" ]]; then
        cd /tapdata-source/tapshell
        bash register-all-connectors.sh
        echo "register_connector_complete !"
        cp ../build/test.sh ./
        cp -r ../build/test ./
        chmod u+x test.sh
        bash ./test.sh
        if [[ $? -ne 0 ]]; then
            exit 127
        else
            exit 0
        fi
    fi
}

_main() {
    start_mongo
    start_server
    if [[ $mode != "test" ]]; then
        sleep infinity
    fi
}

_main "$@"
