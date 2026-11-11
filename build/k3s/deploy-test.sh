#!/usr/bin/env bash
set -euo pipefail
# Usage: deploy-test.sh IMAGE NAMESPACE [CONTEXT]
image=${1:?Usage: deploy-test.sh IMAGE NAMESPACE [CONTEXT]}
namespace=${2:?Provide a dedicated test namespace}
context=${3:-k3s-193}
[[ "$namespace" =~ ^tapdata-ci-[a-z0-9-]+$ ]] || { echo 'Namespace must start with tapdata-ci-' >&2; exit 1; }
[[ "$image" =~ ^[a-zA-Z0-9./:@_-]+$ ]] || exit 1
node=${TAPDATA_TEST_NODE:-tapserver193}
[[ "$node" =~ ^[a-zA-Z0-9.-]+$ ]] || exit 1
k=(kubectl --context "$context" -n "$namespace")
failed() {
  rc=$?
  if ((rc != 0)); then
    "${k[@]}" get pods,events -o wide >&2 || true
    "${k[@]}" logs deploy/tapdata --tail=80 >&2 || true
  fi
  exit "$rc"
}
trap failed EXIT
kubectl --context "$context" create namespace "$namespace" --dry-run=client -o yaml | kubectl --context "$context" apply -f -
# Reuse the existing Harbor pull credential without printing it or writing it to disk.
if [[ "$image" == harbor.internal.tapdata.io/* ]]; then
  kubectl --context "$context" -n "${HARBOR_SECRET_NAMESPACE:-taptest-dbs}" get secret "${HARBOR_SECRET_NAME:-harbor-registry-auth}" -o json |
    python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({"apiVersion":"v1","kind":"Secret","metadata":{"name":"harbor-pull","namespace":sys.argv[1]},"type":s["type"],"data":s["data"]}))' "$namespace" |
    "${k[@]}" apply -f -
fi
cat <<YAML | "${k[@]}" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata: {name: mongo}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: mongo}}
  template:
    metadata: {labels: {app: mongo}}
    spec:
      nodeSelector: {kubernetes.io/hostname: "$node"}
      containers:
        - name: mongo
          image: mongo:6.0
          args: [mongod, --bind_ip_all, --replSet, rs0]
          volumeMounts: [{name: data, mountPath: /data/db}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: mongo}
spec:
  selector: {app: mongo}
  ports: [{port: 27017, targetPort: 27017}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: tapdata}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: tapdata}}
  template:
    metadata: {labels: {app: tapdata}}
    spec:
      nodeSelector: {kubernetes.io/hostname: "$node"}
      imagePullSecrets: [{name: harbor-pull}]
      containers:
        - name: tapdata
          image: "$image"
          env:
            - {name: MONGO_URI, value: "mongodb://mongo:27017/tapdata?replicaSet=rs0"}
            - {name: LAUNCH_SUPERVISOR, value: "true"}
          ports: [{containerPort: 3030}]
          startupProbe:
            httpGet: {path: /, port: 3030}
            periodSeconds: 10
            failureThreshold: 90
          readinessProbe:
            httpGet: {path: /, port: 3030}
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata: {name: tapdata}
spec:
  type: NodePort
  selector: {app: tapdata}
  ports: [{port: 3030, targetPort: 3030}]
YAML
"${k[@]}" rollout status deploy/mongo --timeout=180s
"${k[@]}" exec deploy/mongo -- mongosh --quiet --eval 'try { rs.status() } catch(e) { if(e.code === 94) { rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]}) } else { throw e } }'
# Wait for a writable primary before registration.
for ((i=0; i<60; i++)); do
  if "${k[@]}" exec deploy/mongo -- mongosh --quiet --eval 'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null; then break; fi
  sleep 2
done
"${k[@]}" exec deploy/mongo -- mongosh --quiet --eval 'quit(db.hello().isWritablePrimary ? 0 : 1)'
"${k[@]}" rollout status deploy/tapdata --timeout=900s
# Ready HTTP alone is insufficient: wait for connector registration and engine start.
for ((i=0; i<90; i++)); do
  if "${k[@]}" exec deploy/tapdata -- bash -c 'cd /tapdata/apps; supervisorctl -c supervisor/supervisord.conf status manager iengine' | awk '/RUNNING/{n++} END{exit n==2 ? 0 : 1}'; then break; fi
  sleep 5
done
"${k[@]}" exec deploy/tapdata -- bash -c 'cd /tapdata/apps; supervisorctl -c supervisor/supervisord.conf status manager iengine'
"${k[@]}" logs deploy/tapdata --tail=20
"${k[@]}" get pods,svc -o wide
"${k[@]}" exec deploy/mongo -- mongosh --quiet tapdata --eval 'printjson({definitions:db.MetadataDefinition.countDocuments(),workers:db.Workers.countDocuments()}); quit(db.MetadataDefinition.countDocuments()>0 && db.Workers.countDocuments()>0 ? 0 : 1)'
restarts=$("${k[@]}" get pods -l app=tapdata -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')
[[ "$restarts" == 0 ]] || { echo "Tapdata restarted $restarts times; inspect logs" >&2; exit 1; }
"${k[@]}" get pods -l app=tapdata -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'
echo "Validation complete. Inspect warnings and run a data task before production use."
