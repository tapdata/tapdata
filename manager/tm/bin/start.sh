#!/bin/bash

WORK_DIR=$(pwd)
TM_HOME=$(cd "$(dirname "$0")/.." && pwd)
cat <<_END_

Worker directory:              $WORK_DIR
DRS home directory:            $TM_HOME

_END_

JAVA_OPTS="$JAVA_OPTS -Xms1g -Xmx4g -XX:CompressedClassSpaceSize=512m -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m"

CLASSPATH="$TM_HOME/lib/tm-2.0.0-11-16.jar"
APP_PARAMS="--spring.config.additional-location=file:$TM_HOME/conf/ --logging.config=file:$TM_HOME/conf/logback-tm.xml"

TM_CMD="$JAVA_HOME/bin/java $JAVA_OPTS -jar -server $CLASSPATH $APP_PARAMS"

exec "$TM_CMD"
