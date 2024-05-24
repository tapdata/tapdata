#!/bin/bash

SCRIPT_BASE_DIR=$(dirname "$0")
cd $SCRIPT_BASE_DIR

components=${1:-"all"}

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

help() {
    print_message "Usage: stop.sh [all|manager|iengine|mongodb]" "green" true
    print_message "Stop Manager and IEngine services." "cyan" false
    print_message "  [all]: stop manager and service" "cyan" false
    print_message "  manager: stop Manager service" "cyan" false
    print_message "  iengine: stop IEngine service" "cyan" false
    print_message "  mongodb: stop MongoDB service" "cyan" false
}

print_message "Stopping Manager and IEngine..." "green" true

if [[ $components == "help" || $components == "--help" || $components == "-h" ]]; then
    help
    exit 0
fi

stop_service() {
    local service_name="$1"
    if [[ -f .$service_name.pid ]]; then
        ps -p $(cat .$service_name.pid) > /dev/null && kill -9 $(cat .$service_name.pid)
        print_message "$service_name stopped." "green" false
        rm -rf .$service_name.pid
    else
        print_message "$service_name not running, or no .$service_name.pid file found." "yellow" false
    fi
}

if [[ `cat .launch_supervisor` == "false" ]]; then

    if [[ $components == "all" ]]; then
        # stop manager
        stop_service "manager"
        # stop iengine
        stop_service "iengine"
    elif [[ $components == "manager" ]]; then
        # stop manager
        stop_service "manager"
    elif [[ $components == "iengine" ]]; then
        # stop iengine
        stop_service "iengine"
    elif [[ $components == "mongodb" ]]; then
        # stop mongodb
        stop_service "mongodb"
    fi
else
    if [[ $components == "all" ]]; then
        # stop manager and iengine
        supervisorctl -c supervisor/supervisord.conf stop iengine
        supervisorctl -c supervisor/supervisord.conf stop manager
    elif [[ $components == "manager" ]]; then
        # stop manager
        supervisorctl -c supervisor/supervisord.conf stop manager
    elif [[ $components == "iengine" ]]; then
        # stop iengine
        supervisorctl -c supervisor/supervisord.conf stop iengine
    elif [[ $components == "supervisord" ]]; then
        # stop supervisord
        supervisorctl -c supervisor/supervisord.conf shutdown
    elif [[ $components == "mongodb" ]]; then
        # stop mongodb
        supervisorctl -c supervisor/supervisord.conf stop mongodb
    fi
fi
