#!/usr/bin/env bash

mongouri=$1
appName="tm*.jar"
conf=conf
lib=lib
ulimit -c unlimited

function start()
{
    count=`ps -ef |grep java|grep $appName|wc -l`
    if [ $count != 0 ];then
        echo "Maybe $appName is running, please check it..."
    else
        echo "The $appName is starting..."
        nohup java -jar -server ${lib}/tm-*.jar --spring.config.additional-location=file:${conf}/ --logging.config=file:${conf}/logback.xml -Dspring.data.mongodb.uri=${mongouri} &> logs/nohup.out &
    fi
}

start
