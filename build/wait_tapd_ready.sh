i="."
while [[ 1 ]]; do
  if [[ $i == "..............." ]]; then
    break
  fi
  sleep 15
  i=$i"."
  docker logs tapd|grep register_connector_complete
  if [[ $? -eq 0 ]]; then
    echo "tapdata container ready!"
    exit 0
  fi
  echo "still wait tapdata container ready..."
done
echo "tapdata container not ready, please check manager logs ..."
docker exec -it tapd bash -c "cat /tapdata/apps/manager/logs/*"
exit 1
