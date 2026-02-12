#!/bin/sh
set -e
# TM Local Startup Script

# Handle signal
trap 'kill $(jobs -p)' EXIT

mkdir -p logs
touch logs/tm.log

echo "Starting TM (Local Build)..."
# Use JAVA_OPTS from env or default
JAVA_OPTS=${JAVA_OPTS:-"-Xmx4g"}

# Start TM
# Note: Ensure all required JVM options (add-opens, etc.) are included in JAVA_OPTS or added here if missing from env
java $JAVA_OPTS -jar -Dserver.port=3000 -server lib/tm.jar \
  --spring.config.additional-location=file:conf/ \
  --logging.config=file:conf/logback.xml \
  --spring.data.mongodb.default.uri="${SPRING_DATA_MONGODB_URI}" \
  --spring.data.mongodb.obs.uri="${SPRING_DATA_MONGODB_OBS_URI:-$SPRING_DATA_MONGODB_URI}" \
  --spring.data.mongodb.log.uri="${SPRING_DATA_MONGODB_LOG_URI:-$SPRING_DATA_MONGODB_URI}" \
  > logs/tm.log 2>&1 &

TM_PID=$!
tail -f logs/tm.log &

wait $TM_PID
