#!/bin/bash

SCRIPT_BASE_DIR=$(dirname "$0")
cd $SCRIPT_BASE_DIR

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

service_status() {
    local service_name="$1"
    if [[ -f .$service_name.pid ]]; then
        ps -p $(cat .$service_name.pid) > /dev/null && print_message "$service_name running, pid $(cat .$service_name.pid)" "green" false
        if [[ $? -ne 0 ]]; then
            print_message "$service_name not running." "yellow" false
        fi
    else
        echo "$service_name not running, or no .$service_name.pid file found."
    fi
}

if [[ `cat .launch_supervisor` == 'true' ]]; then
    supervisorctl -c supervisor/supervisord.conf status
else
    service_status "manager"
    service_status "iengine"
    service_status "mongodb"
fi