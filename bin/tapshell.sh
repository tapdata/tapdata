#!/bin/bash
basepath=$(cd `dirname $0`; pwd)

. $basepath/../build/env.sh
. $basepath/../build/log.sh

container_name=$dev_container_name
tapshell_path="/tapdata/apps/tapshell"

docker ps|grep $container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    container_name=$use_container_name
    tapshell_path="/tapdata/apps/tapshell"
    docker ps|grep $container_name &> /dev/null
    if [[ $? -ne 0 ]]; then
        error "no running tapdata container found: $dev_container_name, please run bash build/quick-dev.sh or bash build/quick-use.sh first"
    fi
fi
docker exec -it $container_name bash -c "cd $tapshell_path && bash cli.sh"
