#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

server=`cat $basepath/config.ini |grep "server"|awk -F ' ' '{print $3}'`
access_code=`cat $basepath/config.ini |grep "access_code"|awk -F ' ' '{print $3}'`

if [[ -f ".register" ]]; then
    echo "init connectors register finished, if you want to register your own connector, please run below command:"
    echo "docker exec -i tapdata-all-in-one bash -c \"cd /tapdata-source/ && ./build/pdk register -a $access_code -f GA -t http://$server \$datasource_jar\""
    exit 0
fi

echo "waiting for remote server: $server start..."
while [[ 1 ]]; do
    sleep 2
    curl $server &> /dev/null
    if [[ $? -ne 0 ]]; then
        continue
    else
        break
    fi
done

echo "start register connections with access_code: $access_code to server: http://$server"
cd $basepath/../
pwd
for i in `ls connectors/dist|grep -v tdd-connector`; do
    ./build/pdk register -a $access_code -f GA -t http://$server connectors/dist/$i | grep registered
done

touch "$basepath/.register"
exit 0
