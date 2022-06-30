#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
. $basepath/env.sh
basepath=$(cd `dirname $0`; pwd)
cd $basepath

tag=`cat image/tag`
x=`docker images $tag|wc -l`
if [[ $x -eq 1 ]]; then
    docker pull $tag
fi

docker ps|grep $use_container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    docker run -e mode=use -itd --name=$use_container_name `cat image/tag` bash
fi
docker exec -it $use_container_name bash -c "cd /tapdata/apps/tapshell && bash cli.sh"
