#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
sourcepath=$(cd `dirname $0`/../; pwd)
. $basepath/env.sh
. $basepath/log.sh
cd $basepath

tag=`cat image/tag`
x=`docker images $tag|wc -l`
if [[ $x -eq 1 ]]; then
    docker pull $tag
fi

docker ps|grep $use_container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    docker run -itd -p 3000:33000 --name=$use_container_name $tag
    if [[ $? -ne 0 ]]; then
        error "docker run failed, please check your docker env"
        exit 1
    fi
    docker logs -f $use_container_name
else
    info "All Done, please visit http://localhost:3000 or run 'bash tapshell/cli.sh' to enter container"
fi