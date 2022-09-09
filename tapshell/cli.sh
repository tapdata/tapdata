#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

export LC_ALL="en_US.utf8"
error() {
    echo -e `date`"\033[31m $1 \033[0m"
    exit -1
}

info() {
    echo -e `date`"\033[36m $1 \033[0m"
}

notice() {
    echo -e `date`"\033[32m $1 \033[0m"
}

warn() {
    echo -e `date`"\033[33m $1 \033[0m"
}
which ipython &> /dev/null
if [[ $? -ne 0 ]]; then
    error "NO ipython find, please install python3 and ipython manually before using idaas cli"
fi

server=`cat $basepath/config.ini |grep "server"|awk -F ' ' '{print $3}'`
info "waiting for remote manager: $server start..."

while [[ 1 ]]; do
    sleep 1
    curl $server &> /dev/null
    if [[ $? -ne 0 ]]; then
        continue
    else
        break
    fi
done

info "Welcome to Tapdata Live Data Platform, Enjoy Your Data Trip !"
ipython3 --profile-dir=$basepath/tapdata_cli --profile=ipython_config -i $basepath/tapdata_cli/cli.py --no-banner
