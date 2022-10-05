#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
sourcepath=$(cd `dirname $0`/../; pwd)

. $basepath/env.sh

cd $basepath

docker ps|grep $dev_container_name &> /dev/null
if [[ $? -ne 0 ]]; then
    tag=`cat image/tag`
    x=`docker images $tag|wc -l`
    if [[ $x -eq 1 ]]; then
        cd ../
        bash build/build.sh -c iengine
        bash build/build.sh -c manager
        bash build/build.sh -c plugin-kit
        bash build/build.sh -c connectors
        bash build/build.sh -p 1 -o image
    fi


    test -f $sourcepath/build/test/.env
    if [[ $? -ne 0 ]]; then
        echo $TEST_DATABASE | base64 -di > $sourcepath/build/test/.env
    fi

    cd $basepath
    docker run -e mode=test -e BRANCH=$BRANCH -e RUN_SIGN=$RUN_SIGN -e RUN_ID=$RUN_ID -e RUN_NUMBER=$RUN_NUMBER \
               -p 13000:3000 -p 27017:27017 \
               -v $sourcepath:/tapdata-source/ \
               -i \
               --name=$dev_container_name `cat image/tag` \
               bash -c "/tapdata/docker-entrypoint.sh"
    exit $?
fi
