#!/usr/bin/env bash

killForceFlag=$1
appName="tm*.jar"
function status()
{
    appId=`ps -ef |grep java|grep $appName|awk '{print $2}'`
    if [ -z $appId ]
    then
        echo -e "$appName Not running "
    else
        echo -e "$appName Running [$appId] "
    fi
}

status
