#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath/../

. $basepath/env.sh

bash build/build.sh -d 1

docker rmi -f `cat $basepath/image/tag`
