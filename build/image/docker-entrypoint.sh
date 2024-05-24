#! /bin/bash

SCRIPT_BASE_DIR=$(dirname "$0")

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

install_supervisor() {
    print_message "Supervisor Not Found, it's better to install supervisor first" "yellow" false
    # install by pip/pip3
    print_message "Try to installing Supervisor by pip/pip3" "yellow" false
    which pip > /dev/null
    if [[ $? -ne 0 ]]; then
        which pip3 > /dev/null
        if [[ $? -ne 0 ]]; then
            print_message "pip or pip3 Not Found" "yellow" false
            export LAUNCH_SUPERVISOR=false
        else
            pip3 install supervisor
        fi
    else
        pip install supervisor
    fi
    if [[ $? -eq 0 ]]; then
        which supervisord > /dev/null
        if [[ $? -eq 0 ]]; then
            print_message "Install Supervisor Success" "green" false
            export LAUNCH_SUPERVISOR=true
            return 0
        fi
    fi
    print_message "Install Supervisor by pip/pip3 Failed" "yellow" false
    print_message "Try to install by system package manager" "yellow" false
    # Linux or Macos
    if [[ -f /etc/redhat-release ]]; then
        yum install -y supervisor
    elif [[ -f /etc/lsb-release ]]; then
        apt-get install -y supervisor
    elif [[ $(uname) == "Darwin" ]]; then
        brew install supervisor
    else
        print_message "Not Support OS" "yellow" false
        export LAUNCH_SUPERVISOR=false
        return 1
    fi
    if [[ $? -ne 0 ]]; then
        print_message "Install Supervisor Failed" "yellow" false
        export LAUNCH_SUPERVISOR=false
        return 1
    else
        which supervisord > /dev/null
        if [[ $? -ne 0 ]]; then
            print_message "Install Supervisor Failed" "yellow" false
            export LAUNCH_SUPERVISOR=false
            return 1
        fi
        export LAUNCH_SUPERVISOR=true
        print_message "Install Supervisor Success" "green" false
        return 0
    fi
}

start_supervisord() {
    # install supervisor first
    which supervisord > /dev/null
    if [[ $? -ne 0 ]]; then
        install_supervisor
    fi
    if [[ $LAUNCH_SUPERVISOR == "false" ]]; then
        print_message "Skip Start Supervisor" "yellow" false
        echo 'false' > .launch_supervisor
        return 0
    else
        echo 'true' > .launch_supervisor
    fi

    # if supervisord is already running, skip start
    ps -ef | grep -v grep | grep supervisord > /dev/null
    if [[ $? -eq 0 ]]; then
        print_message "Supervisor is Already Running" "green" false
        return 0
    fi

    # create paths
    mkdir -p supervisor/logs/ supervisor/run/ supervisor/conf.d/
    touch supervisor/logs/supervisord.log # create supervisord log file
    touch supervisor/logs/manager.log supervisor/logs/manager.err # create manager log file
    touch supervisor/logs/iengine.log supervisor/logs/iengine.err # create iengine log file
    touch supervisor/logs/mongodb.log supervisor/logs/mongodb.err # create mongodb log file
    # start supervisor
    supervisord -c supervisor/supervisord.conf
    # check supervisor start
    ps -ef | grep -v grep | grep supervisord > /dev/null
    if [[ $? -ne 0 ]]; then
        export LAUNCH_SUPERVISOR=false
        print_message "Supervisor Start Failed" "red" false
        return 1
    fi
}

get_env() {

    # Set default env
    export MONGO_URI=${MONGO_URI:-"mongodb://127.0.0.1:27017/tapdata"}  # mongodb uri
    export dbMem=${dbMem:-"1"} # db memory
    export engineMem=${engineMem:-"1G"} # engine memory
    export managerMem=${managerMem:-"1G"} # manager memory
    export tm_port=${tm_port:-"3030"} # manager port
    export ACCESS_CODE=${ACCESS_CODE:-"3324cfdf-7d3e-4792-bd32-571638d4562f"}  # access code
    export LAUNCH_SUPERVISOR=${LAUNCH_SUPERVISOR:-"false"}  # launch supervisor

    # Export env
    export MONGO_URI dbMem engineMem managerMem tm_port ACCESS_CODE

cat <<EOF
MONGO_URI         : $MONGO_URI
dbMem             : $dbMem
engineMem         : $engineMem
managerMem        : $managerMem
tm_port           : $tm_port
ACCESS_CODE       : $ACCESS_CODE
LAUNCH_SUPERVISOR : $LAUNCH_SUPERVISOR
EOF

    if [[ $LAUNCH_SUPERVISOR == "true" ]]; then
        start_supervisord
        echo 'true' > .launch_supervisor
    else
        echo 'false' > .launch_supervisor
    fi
}

start_mongo() {
    # start a mongodb replSet

    mkdir -p /tapdata/data/logs /tapdata/data/db/
    if [[ $LAUNCH_SUPERVISOR == "true" ]]; then
        supervisorctl -c supervisor/supervisord.conf start mongodb
    else
        mongod --dbpath=/tapdata/data/db/ --wiredTigerCacheSizeGB=$dbMem --replSet=rs0 --bind_ip_all --logpath=/tapdata/data/logs/mongod.log --fork
        sleep 2
        pgrep mongod > .mongodb.pid
    fi
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
        curl --fail "http://localhost:$tm_port" &> /dev/null
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
    java -jar $dir/lib/pdk-deploy.jar register -a $ACCESS_CODE -t http://localhost:$tm_port $dir/connectors/dist 2>&1
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

start_tm() {
    if [[ $LAUNCH_SUPERVISOR == "true" ]]; then
        supervisorctl -c supervisor/supervisord.conf start manager
    else
        nohup java -Xmx$managerMem -jar -Dserver.port=$tm_port -server components/tm.jar --spring.config.additional-location=file:etc/ --logging.config=file:etc/logback.xml --spring.data.mongodb.default.uri=$MONGO_URI --spring.data.mongodb.obs.uri=$MONGO_URI --spring.data.mongodb.log.uri=$MONGO_URI &> logs/nohup.out &
        echo $! > .manager.pid
    fi
}

start_iengine() {
    if [[ $LAUNCH_SUPERVISOR == "true" ]]; then
        supervisorctl -c supervisor/supervisord.conf start iengine
    else
        mkdir -p logs/iengine/ && touch logs/iengine/tapdata-agent.jar.log
        export app_type="DAAS"
        export backend_url="http://127.0.0.1:$tm_port/api/"
        export TAPDATA_MONGO_URI=$MONGO_URI
        nohup java -Xmx$engineMem -jar components/tapdata-agent.jar &> logs/iengine/tapdata-agent.jar.log &
        echo $! > .iengine.pid
    fi
}

start_server() {
    # 1. start manager server
    # 2. register all connectors
    # 3. start iengine server
    #
    if [[ $LAUNCH_SUPERVISOR == "true" ]]; then
        supervisorctl -c supervisor/supervisord.conf reread && supervisorctl -c supervisor/supervisord.conf update
    fi
    # 1. start manager server
    print_message "Start Manager Server" "blue" false
    start_tm
    # 2. register all connectors
    # waiting for manager server start
    exec_with_log wait_tm_start "Waiting for Manager Server Start" "blue" || return 1
    # Register all connectors
    exec_with_log register_connectors "Register all connectors" "blue" || return 1
    # 3. start iengine server
    start_iengine
}

unzip_files() {
    if [[ -f /tapdata/apps/connectors/dist.tar.gz ]]; then
      tar xzf /tapdata/apps/connectors/dist.tar.gz -C /tapdata/apps/connectors
      rm -rf /tapdata/apps/connectors/dist.tar.gz
    elif [[ -f ./connectors/dist.tar.gz ]]; then
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
    cd $SCRIPT_BASE_DIR
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
    # 3. start mongo if $MONGO_URI is not set and in docker
    ps -ef | grep -v grep | grep mongo > /dev/null
    if [[ -f /.dockerenv && $? -ne 0 ]]; then
        print_message ">>> Start Mongo [START]" "green" true
        start_mongo
        ps -ef | grep -v grep | grep mongo > /dev/null
        if [[ $? -ne 0 ]]; then
            print_message "<<< Mongodb is Not Running" "red" true
            exit 1
        fi
        print_message "<<< Mongodb is Already Running" "green" true
    fi
    # 4. start tm server, register connectors and start iengine
    print_message ">>> Start Server [START]" "green" true
    start_server
    if [[ $? -ne 0 ]]; then
        print_message "<<< Start Server [FAILED]" "red" true
        exit 1
    fi
    print_message "<<< Start Server [SUCCESS]" "green" true
    # Print Visit Url
    print_message "All Done, Please Visit http://localhost:$tm_port" "green" true

    # 5. hold the container
    if [[ -f /.dockerenv ]]; then
        sleep infinity
    fi
}

_main "$@"
