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
    docker run -v `pwd`/data:/data/db/ -e mode=use -itd --name=$use_container_name `cat image/tag` bash
fi

info "tapdata all in one env started, you can use 'bash $sourcepath/bin/tapshell.sh' go into terminal env now..."
bash $sourcepath/bin/tapshell.sh
