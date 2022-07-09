#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

server=`cat $basepath/config.ini |grep "server"|awk -F ' ' '{print $3}'`
port=`echo $server | awk -F':' '{print $2}'`
access_code=`cat $basepath/config.ini |grep "access_code"|awk -F ' ' '{print $3}'`

error() {
    echo -e "["`date +'%D %T'`"]" ${header}:"\033[31m $1 \033[0m"
    exit 1
}

which pytest &> /dev/null
if [[ $? -ne 0 ]]; then
    error "no pytest module found, please run pip install pytest first"
fi

test -f $basepath/test/.env
if [[ $? -ne 0 ]]; then
    echo $TEST_DATABASE > $basepath/test/.env
fi

for i in $(seq 1 30); do
    lsof -i:$port
    if [[ $? -eq 0 ]]; then
        break
    fi
    sleep 1
done

cd $basepath/test/
pytest

if [[ $? -ne 0 ]]; then
    exit 1
fi