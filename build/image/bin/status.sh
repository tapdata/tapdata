#!/bin/bash

SCRIPT_BASE_DIR=$(dirname "$0")
cd $SCRIPT_BASE_DIR

if [[ `cat .launch_supervisor` == 'true' ]]; then
    supervisorctl -c supervisor/supervisord.conf status
else
    if [[ -f .manager.pid ]]; then
        ps -p $(cat .manager.pid)
    fi
    if [[ -f .iengine.pid ]]; then
        ps -p $(cat .iengine.pid)
    fi
fi