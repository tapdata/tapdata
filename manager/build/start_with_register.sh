#!/bin/bash

# Start TM in background
echo "Starting TM..."
nohup java $JVM_OPTS -Xmx${TM_HEAP_SIZE:-4G} -Dserver.port=$SERVER_PORT -server -jar lib/tm.jar --spring.config.additional-location=file:conf/ --logging.config=file:conf/logback.xml --spring.data.mongodb.default.uri=${DEFAULT_MONGO_URI} --spring.data.mongodb.obs.uri=${OBS_MONGO_URI} --spring.data.mongodb.log.uri=${LOG_MONGO_URI} > logs/tm.log 2>&1 &
TM_PID=$!

# Wait for TM to be ready
echo "Waiting for TM to start (checking localhost:3030)..."
i=0
while [ $i -lt 60 ]; do
    if curl -s http://localhost:3030/ >/dev/null; then
        echo "TM is up!"
        break
    fi
    echo "Waiting for TM... ($i)"
    sleep 5
    i=$((i+1))
done

# Register connector
echo "Registering mongodb-connector..."
# Use absolute path for safety
JAR_PATH="$(pwd)/connectors/dist/mongodb-connector.jar"
if [ -f "$JAR_PATH" ]; then
    java -jar lib/pdk-deploy.jar register -a 3324cfdf-7d3e-4792-bd32-571638d4562f -t http://localhost:3030 "$JAR_PATH" 2>&1 | tee logs/connector-register.log
else
    echo "Error: Connector jar not found at $JAR_PATH"
fi

# Keep container alive by waiting for TM
wait $TM_PID
