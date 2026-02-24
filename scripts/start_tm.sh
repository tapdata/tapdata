#!/bin/sh
if [ -f connectors/dist.tar.gz ]; then
  tar -xzf connectors/dist.tar.gz -C connectors/
fi
mkdir -p logs
touch logs/tm.log
java -Xms4g -Xmx4g -jar -Dserver.port=3030 -server components/tm.jar --spring.config.additional-location=file:etc/ --logging.config=file:etc/logback.xml --spring.data.mongodb.default.uri=mongodb://tapdata-mongo-official:27017/tapdata?replicaSet=rs0 --spring.data.mongodb.obs.uri=mongodb://tapdata-mongo-official:27017/tapdata?replicaSet=rs0 --spring.data.mongodb.log.uri=mongodb://tapdata-mongo-official:27017/tapdata?replicaSet=rs0 > logs/tm.log 2>&1 &
TM_PID=$!
echo "Waiting for TM to start..."
until curl -s http://localhost:3030/api/ > /dev/null; do
  sleep 5
  echo "Waiting for TM..."
done
echo "Registering connectors..."
java -jar lib/pdk-deploy.jar register -a 3324cfdf-7d3e-4792-bd32-571638d4562f -t http://localhost:3030 connectors/dist/mongodb-connector-v1.0-SNAPSHOT.jar
echo "Registration complete. Tailing TM logs..."
tail -f logs/tm.log &
wait $TM_PID