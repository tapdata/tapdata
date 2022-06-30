#!/usr/bin/env bash

killForceFlag=$1
appName="tm*.jar"
function stop()
{
    appId=`ps -ef |grep java|grep $appName|awk '{print $2}'`
    if [ -z $appId ]
    then
        echo "Maybe $appName not running, please check it..."
    else
        echo -n "The $appName is stopping..."
        if [ "$killForceFlag" == "-f" ]
        then
            echo "by force"
            kill -9 $appId
        else
            echo
            kill $appId
        fi
    fi
}

stop
