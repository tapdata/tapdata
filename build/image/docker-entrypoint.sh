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
    # Set default env

    MONGO_URI=${MONGO_URI}  # mongodb uri
    ACCESS_CODE=${ACCESS_CODE:-"3324cfdf-7d3e-4792-bd32-571638d4562f"}  # access code

    print_message "MONGO_URI  :   $MONGO_URI" "blue" false
    print_message "ACCESS_CODE:   $ACCESS_CODE" "blue" false
}

start_mongo() {
    # start a mongodb replSet

    mkdir -p /tapdata/data/logs /tapdata/data/db/
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

wait_tm_start() {
    local timeout=$((SECONDS + 300))  # timeout 300 seconds
    local counter=0
    while [[ $SECONDS -lt $timeout ]]; do
        local seconds_left=$((timeout - SECONDS))
        printf "\r* Wait Starting, Left %02d / 300 Seconds..." "$seconds_left"
        sleep 1
        curl --fail "http://localhost:30000" &> /dev/null
        if [[ $? -ne 0 ]]; then
            continue
        else
            printf "\n~ Manager server started\n"
            return 0
        fi
        counter=$((counter + 1))
    done
    printf "\n~ Manager Starting Timeout\n"
    return 1
}

exec_with_log() {
    command=$1
    log=$2
    color=$3

    print_message "~ $log" "$color" false
    eval $command
    if [[ $? -ne 0 ]]; then
        print_message "~ $log Failed" "red" false
        return 1
    else
        print_message "~ $log Success" "$color" false
    fi
}

_register_connectors() {
    print_message "* Register Connector: $i" "blue" false
    java -jar $dir/lib/pdk-deploy.jar register -a $ACCESS_CODE -f GA -t http://localhost:3000 $dir/connectors/dist 2>&1
    if [[ $? -ne 0 ]]; then
        print_message "* Register Connector: $i Failed" "red" false
        exit 1
    else
        print_message "* Register Connector: $i Success" "blue" false
    fi
}

register_connectors() {
    if [[ -d /tapdata/apps/connectors ]]; then
      dir=/tapdata/apps
    else
      dir=./
    fi
    _register_connectors
}

start_server() {
    # 1. start manager server
    # 2. register all connectors
    # 3. start iengine server
    #
    # 1. start manager server
    if [[ -d /tapdata/apps/manager ]]; then
      mkdir -p /tapdata/apps/logs /tapdata/apps/logs/iengine
      exec_with_log "cd /tapdata/apps/ && bash bin/manager/start.sh $MONGO_URI" "Start Manager Server" "blue" || return 1
    else
      mkdir -p ./logs ./logs/iengine
      exec_with_log "bash bin/manager/start.sh $MONGO_URI" "Start Manager Server" "blue" || return 1
    fi
    # 2. register all connectors
    # waiting for manager server start
    exec_with_log wait_tm_start "Waiting for Manager Server Start" "blue" || return 1
    # Register all connectors
    exec_with_log register_connectors "Register all connectors" "blue" || return 1
    # 3. start iengine server
    if [[ -d /tapdata/apps/iengine ]]; then
      exec_with_log "cd /tapdata/apps/ && bash bin/iengine/start.sh" "Start Iengine Server" "blue" || return 1
    else
      exec_with_log "bash bin/iengine/start.sh" "Start Iengine Server" "blue" || return 1
    fi
}

unzip_files() {
    if [[ -d /tapdata/apps/connectors/ ]]; then
      tar xzf /tapdata/apps/connectors/dist.tar.gz -C /tapdata/apps/connectors
      rm -rf /tapdata/apps/connectors/dist.tar.gz
    elif [[ -d ./connectors ]]; then
      tar xzf ./connectors/dist.tar.gz -C connectors
      rm -rf ./connectors/dist.tar.gz
    fi
}

cat << "EOF"
  _______       _____  _____       _______
 |__   __|/\   |  __ \|  __ \   /\|__   __|/\
    | |  /  \  | |__) | |  | | /  \  | |  /  \
    | | / /\ \ |  ___/| |  | |/ /\ \ | | / /\ \
    | |/ ____ \| |    | |__| / ____ \| |/ ____ \
    |_/_/    \_\_|    |_____/_/    \_\_/_/    \_\
EOF

_main() {
    # 1. get env settings
    # 2. unzip connectors
    # 3. start mongo if $MONGO_URI is not set
    # 4. start tm server, register connectors and start iengine
    # 5. hold the container
    #
    # 1. get env settings
    print_message ">>> Get Env Settings [START]" "green" true
    get_env
    print_message "<<< Get Env Settings [SUCCESS]" "green" true
    # 2. unzip connectors
    print_message ">>> Unzip Connectors [START]" "green" true
    unzip_files
    print_message "<<< Unzip Connectors [SUCCESS]" "green" true
    # 3. start mongo if $MONGO_URI is not set
    print_message ">>> Start Mongo [START]" "green" true
    if [[ -z $MONGO_URI ]]; then
        start_mongo
        ps -ef | grep -v grep | grep mongo > /dev/null
        if [[ $? -ne 0 ]]; then
            print_message "<<< Mongodb is Not Running" "red" true
            exit 1
        fi
        MONGO_URI="mongodb://127.0.0.1:27017/tapdata"
    fi
    print_message "<<< Mongodb is Already Running" "green" true
    # 4. start tm server, register connectors and start iengine
    print_message ">>> Start Server [START]" "green" true
    start_server
    if [[ $? -ne 0 ]]; then
        print_message "<<< Start Server [FAILED]" "red" true
        exit 1
    fi
    print_message "<<< Start Server [SUCCESS]" "green" true
    # Print Visit Url
    print_message "All Done, Please Visit http://localhost:3000" "green" true

    # 5. hold the container
    sleep infinity
}

_main "$@"
