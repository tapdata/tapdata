#!/bin/bash
set -e

# Wait for TM to be ready
echo "Waiting for TM to be ready..."
until curl -s http://tm:${TM_PORT:-3000}/api/ > /dev/null; do
  echo "TM is not ready yet..."
  sleep 5
done
echo "TM is ready"

# Register mongodb-connector
echo "Registering mongodb-connector..."
# pdk.jar requires java 8+ (which we have)
# Usage: java -jar pdk.jar register -t <tm_url> <jar_file>
java -jar /app/pdk/pdk.jar register -t http://tm:${TM_PORT:-3000} /app/pdk/dist/mongodb-connector.jar

echo "Registration complete. Starting Engine..."

# Start Engine
# Use the original CMD command
# java $JVM_OPTS -Xmx2G -jar lib/ie.jar
exec java $JVM_OPTS -Xmx2G -jar lib/ie.jar