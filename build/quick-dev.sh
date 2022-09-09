#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
force=$1
if [[ "x"$force == "x-f" ]]; then
    force=1
fi

. $basepath/env.sh
. $basepath/log.sh
basepath=$(cd `dirname $0`; pwd)
sourcepath=$(cd `dirname $0`/../; pwd)
cd $basepath

if [[ $force -eq 1 ]]; then
    docker rm -f $dev_container_name
    docker rm -f $build_container_name
    docker rmi -f `cat image/tag`
fi

docker ps|grep $dev_container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    tag=`cat image/tag`
    x=`docker images $tag|wc -l`
    if [[ $x -eq 1 ]]; then
        cd ../
        bash build/build.sh -c plugin-kit
        bash build/build.sh -c connectors-common
        bash build/build.sh -c manager
        bash build/build.sh -c iengine
        bash build/build.sh -c connectors
        bash build/build.sh -p 1 -o image
    fi
    mkdir -p data
    docker run -e mode=dev -p 13000:3000 -p 27017:27017 -v $sourcepath:/tapdata-source/ -v `pwd`/data:/tapdata/data/db/ -itd --name=$dev_container_name $tag bash
fi

docker exec -it $dev_container_name bash -c "cd /tapdata-source/tapshell && bash register-all-connectors.sh"
cd $basepath/../
info "tapdata all in one env started, you can run bash bin/tapshell.sh go into terminal env now..."
bash bin/tapshell.sh
