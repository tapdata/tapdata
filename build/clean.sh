#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath/../
. $basepath/env.sh
. $basepath/log.sh

info "cleaning build components..."
bash build/build.sh -d 1

info "clean running tapdata container..."
docker rm -f $dev_container_name &> /dev/null
docker rm -f $use_container_name &> /dev/null
docker rm -f $build_container_name &> /dev/null
warn "if you want to clean tapdata container data, just delete path ./data"

info "clean tapdata image..."
docker rmi -f `cat $basepath/image/tag` &> /dev/null
