#!/usr/bin/env bash

mongouri=$1
appName="tm.jar"
conf=etc
lib=components
tm_port=${tm_port:-"3030"}
ulimit -c unlimited

function start()
{
    echo "The $appName is starting..."
    touch logs/nohup.out
    nohup java -jar -Dserver.port=$tm_port -server ${lib}/tm.jar --spring.config.additional-location=file:${conf}/ --logging.config=file:${conf}/logback.xml --spring.data.mongodb.default.uri=${mongouri} --spring.data.mongodb.obs.uri=${mongouri} --spring.data.mongodb.log.uri=${mongouri} &> logs/nohup.out &
}

start
