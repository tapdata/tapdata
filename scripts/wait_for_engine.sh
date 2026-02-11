#!/bin/sh
set -e

# Handle signal
trap 'kill $(jobs -p)' EXIT

# Check for dist.tar.gz and extract if exists
if [ -f connectors/dist.tar.gz ]; then
  tar -xzf connectors/dist.tar.gz -C connectors/
fi

mkdir -p logs
touch logs/agent.log

# Start Engine in background with reduced memory for local dev
echo "Starting Engine..."
# Reduced to 4G for local compatibility. Production should use higher values.
java -Xms4g -Xmx4g -jar components/tapdata-agent.jar > logs/agent.log 2>&1 &
ENGINE_PID=$!

echo "Waiting for TM to start..."
# Use correct hostname 'tapdata-tm-official' instead of 'localhost'
until curl -s http://tapdata-tm-official:${TM_PORT:-3000}/api/ > /dev/null; do
  sleep 5
  echo "Waiting for TM..."
done

echo "Registering connectors..."
java -jar lib/pdk-deploy.jar register -a 3324cfdf-7d3e-4792-bd32-571638d4562f -t http://tapdata-tm-official:${TM_PORT:-3000} connectors/dist

echo "Registration complete. Tailing Engine logs..."
tail -f logs/agent.log &

# Wait for the Engine process. If it dies (e.g. OOM), the script will exit.
wait $ENGINE_PID
