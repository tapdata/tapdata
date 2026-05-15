#!/bin/sh
set -e
# Engine Local Startup Script

# Handle signal
trap 'kill $(jobs -p)' EXIT

mkdir -p logs
touch logs/agent.log

echo "Starting Engine (Local Build)..."
# Use JAVA_OPTS from env or default (4G for local dev)
JAVA_OPTS=${JAVA_OPTS:-"-Xms4g -Xmx4g"}

# Start Engine
# Local build jar is lib/ie.jar
java $JAVA_OPTS -jar lib/ie.jar > logs/agent.log 2>&1 &
ENGINE_PID=$!

echo "Waiting for TM to start..."
# Connect to TM service (hostname: tm)
until curl -s http://tm:3000/api/ > /dev/null; do
  sleep 5
  echo "Waiting for TM..."
done

echo "Registering connectors..."
# Local build has pdk.jar in /app/pdk/ and connectors in /app/pdk/dist/
if [ -f "/app/pdk/pdk.jar" ] && [ -d "/app/pdk/dist" ]; then
    TM_URL=${TM_URL:-http://tm:3000}
    AGENT_ID=${ENGINE_AGENT_ID:-3324cfdf-7d3e-4792-bd32-571638d4562f}
    CONNECTOR_IDS=${TAP_CONNECTORS:-${TM_CONNECTORS:-}}

    echo "TM_URL=$TM_URL"
    echo "ENGINE_AGENT_ID=$AGENT_ID"
    echo "TAP_CONNECTORS=$CONNECTOR_IDS"
    echo "Use command to register connector: java -jar /app/pdk/pdk.jar register -a <agent_id> -t <tm_url> /app/pdk/dist/<connector-id>-connector.jar"

    if [ -n "$CONNECTOR_IDS" ]; then
      echo "$CONNECTOR_IDS" | tr ',' '\n' | while read -r raw; do
        id=$(printf '%s' "$raw" | tr -d '[:space:]')
        [ -z "$id" ] && continue
        jar="/app/pdk/dist/${id}-connector.jar"
        if [ -f "$jar" ]; then
          echo "Registering ${id}-connector..."
          java -jar /app/pdk/pdk.jar register -a "$AGENT_ID" -t "$TM_URL" "$jar"
        else
          echo "Skip ${id}-connector: JAR not found at ${jar}"
        fi
      done
    else
      echo "No TAP_CONNECTORS provided, registering all jars under /app/pdk/dist"
      java -jar /app/pdk/pdk.jar register -a "$AGENT_ID" -t "$TM_URL" /app/pdk/dist
    fi
else
    echo "Warning: pdk.jar or connectors dist not found in /app/pdk/, skipping registration"
fi

echo "Registration complete. Tailing Engine logs..."
tail -f logs/agent.log &

wait $ENGINE_PID
