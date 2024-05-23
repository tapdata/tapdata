#!/bin/bash

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

print_message "Stopping Manager and IEngine..." "cyan" true

if [[ `cat .launch_supervisor` == "false" ]]; then
    # stop manager
    if [[ -f .manager.pid ]]; then
        kill -9 $(cat .manager.pid)
    else
        print_message "Manager not running, or no .manager.pid file found." "yellow" false
    fi
    # stop iengine
    if [[ -f .iengine.pid ]]; then
        kill -9 $(cat .iengine.pid)
    else
        print_message "IEngine not running, or no .iengine.pid file found." "yellow" false
    fi
else
    # stop manager and iengine
    supervisorctl -c supervisor/supervisord.conf stop iengine
    supervisorctl -c supervisor/supervisord.conf stop manager

    # stop supervisord
    supervisorctl -c supervisor/supervisord.conf shutdown
    print_message "Done." "green" false
fi

rm -rf .launch_supervisor
