#! /bin/bash

print_message() {
  # Print message with color and bold
  local message="$1"
  local color="$2"
  local is_bold="$3"

  local reset="\e[0m"
  local color_code=""
  local bold_code=""

  # Set color code
  case $color in
    "red") color_code="\e[31m" ;;
    "green") color_code="\e[32m" ;;
    "yellow") color_code="\e[33m" ;;
    "blue") color_code="\e[34m" ;;
    "magenta") color_code="\e[35m" ;;
    "cyan") color_code="\e[36m" ;;
  esac

  # Set bold code
  if [[ $is_bold == true ]]; then
    bold_code="\e[1m"
  fi

  # Print message with color and bold
  echo -e "${bold_code}${color_code}${message}${reset}"
}

get_env() {
    MONGO_URI=${MONGO_URI}  # mongodb uri
    ACCESS_CODE=${ACCESS_CODE:-"3324cfdf-7d3e-4792-bd32-571638d4562f"}  # access code

    print_message "MONGO_URI: $MONGO_URI" "blue" false
    print_message "ACCESS_CODE: $ACCESS_CODE" "blue" false
}

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
    # 2. register all connectors
    # 3. start iengine server
    #
    # 1. start manager server
    print_message "Start manager server" "yellow" false
    cd /tapdata/apps/manager/ && bash bin/start.sh $MONGO_URI
    if [[ $? -ne 0 ]]; then
        print_message "Start manager server failed" "red" false
        exit 1
    else
        print_message "Start manager server success" "green" false
    fi
    # 2. register all connectors
    # waiting for manager server start
    print_message "Waiting for manager server start" "green" false
    while [[ 1 ]]; do
        sleep 2
        curl "http://localhost:3000" &> /dev/null
        if [[ $? -ne 0 ]]; then
            continue
        else
            print_message "Manager server started" "green" false
            break
        fi
    done
    print_message "Register all connectors" "yellow" false
    for i in `ls /tapdata/apps/connectors/dist/`; do
        print_message "Register connector: $i" "cyan" false
        java -jar /tapdata/apps/lib/pdk.jar register -a $ACCESS_CODE -t http://localhost:3000 /tapdata/apps/connectors/dist/$i
        if [[ $? -ne 0 ]]; then
            print_message "Register connector: $i failed" "red" false
            exit 1
        else
            print_message "Register connector: $i success" "green" false
        fi
    done
    # 3. start iengine server
    print_message "Start iengine server" "yellow" false
    cd /tapdata/apps/iengine/ && bash bin/start.sh
    if [[ $? -ne 0 ]]; then
        print_message "Start iengine server failed" "red" false
        exit 1
    else
        print_message "Start iengine server success" "green" false
    fi
}

unzip_files() {
    tar xzf /tapdata/apps/connectors/dist.tar.gz -C /tapdata/apps/connectors
    rm -rf /tapdata/apps/connectors/dist.tar.gz
}

_main() {
    # 1. get env settings
    # 2. unzip connectors
    # 3. start mongo if $MONGO_URI is not set
    # 4. start tm server, register connectors and start iengine
    # 5. hold the container
    #
    # 1. get env settings
    print_message "Get env settings" "green" true
    get_env
    # 2. unzip connectors
    print_message "Unzip connectors" "green" true
    unzip_files
    # 3. start mongo if $MONGO_URI is not set
    print_message "Start mongo" "green" true
    if [[ -z $MONGO_URI ]]; then
        start_mongo
    fi
    if [[ $? -ne 0 ]]; then
        print_message "Start mongo failed" "red" true
        ps -ef | grep -v grep | grep mongo > /dev/null
        if [[ $? -eq 0 ]]; then
            print_message "Mongodb is already start" "green" false
        else
            print_message "Mongodb is not start" "red" false
            exit 1
        fi
        MONGO_URI="mongodb://127.0.0.1:27017/tapdata"
    else
        print_message "Start mongo success" "green" true
    fi
    # 4. start tm server, register connectors and start iengine
    print_message "Start tm server, register connectors and start iengine" "green" true
    start_server
    if [[ $? -ne 0 ]]; then
        print_message "Start tm server, register connectors and start iengine failed" "red" true
        exit 1
    else
        print_message "Start tm server, register connectors and start iengine success" "green" true
    fi

    # 5. hold the container
    sleep infinity
}

_main "$@"
