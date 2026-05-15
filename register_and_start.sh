#!/bin/bash
set -e

# Wait for TM to be ready
echo "Waiting for TM to be ready..."
until curl -s http://tm:3000/api/ > /dev/null; do
  echo "TM is not ready yet..."
  sleep 5
done
echo "TM is ready"

echo "Use command to register connector: java -jar /app/pdk/pdk.jar register -t http://tm:3000 /app/pdk/dist/<connector-id>-connector.jar.jar"
CONNECTOR_IDS="${TAP_CONNECTORS:-${TM_CONNECTORS:-}}"

register_jar() {
  local jar="$1"
  echo "Registering $(basename "$jar")..."
  java -jar /app/pdk/pdk.jar register -t http://tm:3000 "$jar"
}

if [ -n "$CONNECTOR_IDS" ]; then
  IFS=',' read -r -a IDS <<< "$CONNECTOR_IDS"
  for raw in "${IDS[@]}"; do
    id="$(printf '%s' "$raw" | tr -d '[:space:]')"
    [ -z "$id" ] && continue
    jar="/app/pdk/dist/${id}-connector.jar"
    if [ -f "$jar" ]; then
      register_jar "$jar"
    else
      echo "Skip ${id}-connector: JAR not found at ${jar}"
    fi
  done
else
  shopt -s nullglob
  jars=(/app/pdk/dist/*-connector.jar)
  if [ ${#jars[@]} -eq 0 ]; then
    echo "No connector jars found under /app/pdk/dist"
  else
    for jar in "${jars[@]}"; do
      register_jar "$jar"
    done
  fi
fi

echo "Registration complete. Starting Engine..."

# Start Engine
# Use the original CMD command
# java $JVM_OPTS -Xmx2G -jar lib/ie.jar
exec java $JVM_OPTS -Xmx2G -jar lib/ie.jar
