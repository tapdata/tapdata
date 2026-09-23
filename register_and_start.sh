#!/bin/bash
set -e

# Wait for TM to be ready
echo "Waiting for TM to be ready..."
until curl -s http://tm:3000/api/ > /dev/null; do
  echo "TM is not ready yet..."
  sleep 5
done
echo "TM is ready"

echo "Use command to register connector: java -jar /app/pdk/pdk.jar register -t http://tm:3000 /app/pdk/dist/<connector-id>-connector-*.jar"
CONNECTOR_IDS="${TAP_CONNECTORS:-${TM_CONNECTORS:-}}"

register_jar() {
  local jar="$1"
  local -a auth_args=()
  if [ -n "${TAPDATA_ACCESS_CODE:-}" ]; then
    auth_args=(-a "$TAPDATA_ACCESS_CODE")
  elif [ -n "${TAPDATA_ACCESS_KEY:-}" ] && [ -n "${TAPDATA_SECRET_KEY:-}" ]; then
    auth_args=(-ak "$TAPDATA_ACCESS_KEY" -sk "$TAPDATA_SECRET_KEY")
  elif [ -n "${TAPDATA_PASSWORD:-}" ]; then
    auth_args=(-u "${TAPDATA_USERNAME:-admin@admin.com}" -p "$TAPDATA_PASSWORD")
  else
    echo "Skipping $(basename "$jar"): configure TAPDATA_ACCESS_CODE, TAPDATA_ACCESS_KEY/TAPDATA_SECRET_KEY, or TAPDATA_PASSWORD to register connectors"
    return 0
  fi
  echo "Registering $(basename "$jar")..."
  java -jar /app/pdk/pdk.jar register -t http://tm:3000 "${auth_args[@]}" "$jar"
}

if [ -n "$CONNECTOR_IDS" ]; then
  IFS=',' read -r -a IDS <<< "$CONNECTOR_IDS"
  for raw in "${IDS[@]}"; do
    id="$(printf '%s' "$raw" | tr -d '[:space:]')"
    [ -z "$id" ] && continue
    jar="$(find /app/pdk/dist -maxdepth 1 -type f -name "${id}-connector*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*-original.jar" | sort | tail -n 1)"
    if [ -n "$jar" ] && [ -f "$jar" ]; then
      register_jar "$jar"
    else
      echo "Skip ${id}-connector: JAR not found under /app/pdk/dist"
    fi
  done
else
  shopt -s nullglob
  jars=(/app/pdk/dist/*.jar)
  if [ ${#jars[@]} -eq 0 ]; then
    echo "No connector jars found under /app/pdk/dist"
  else
    for jar in "${jars[@]}"; do
      case "$jar" in
        *-sources.jar|*-javadoc.jar|*-original.jar) continue ;;
      esac
      register_jar "$jar"
    done
  fi
fi

echo "Registration complete. Starting Engine..."

# Start Engine
# Use the original CMD command
# java $JVM_OPTS -Xmx2G -jar lib/ie.jar
exec java $JVM_OPTS -Xmx2G -jar lib/ie.jar
