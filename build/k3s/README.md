# k3s image verification

Run from a machine with kubectl access to the test cluster:

```bash
bash build/k3s/deploy-test.sh \
  harbor.internal.tapdata.io/tapdata/tapdata:NEW_TAG \
  tapdata-ci-NEW-RUN-ID k3s-193
```

Use a fresh `tapdata-ci-...` namespace for each clean-image test. This creates a
single Tapdata instance and a dedicated MongoDB 6 replica set. MongoDB uses
`emptyDir`: this is disposable validation data and is lost if its pod is recreated.
Existing Tapdata namespaces and databases are not modified. Re-running with the
same namespace reuses its current MongoDB and registration state.

`TAPDATA_TEST_NODE` defaults to `tapserver193`, which resolves the internal Harbor.
For Harbor images the script copies the existing pull secret from
`HARBOR_SECRET_NAMESPACE`/`HARBOR_SECRET_NAME` (defaults:
`taptest-dbs`/`harbor-registry-auth`) without displaying its contents.

The image's own entrypoint and JVM configuration are tested without deployment
workarounds. The script waits for MongoDB PRIMARY, HTTP readiness, both manager
and engine supervisor processes, and nonempty connector/worker collections.
Failures return nonzero and print pod events and recent container logs.
The NodePort URL is `http://<node-IP>:<service-port>`; the final service listing
shows the allocated port. Actual source/target data synchronization needs a
separate test task. To inspect application log files:

```bash
kubectl --context k3s-193 -n tapdata-ci-NEW-RUN-ID exec deploy/tapdata -- \
  bash -c 'cd /tapdata/apps; tail -100 supervisor/logs/manager.log; tail -100 supervisor/logs/iengine.log'
```

After preserving any needed evidence, remove only that disposable namespace:

```bash
kubectl --context k3s-193 delete namespace tapdata-ci-NEW-RUN-ID
```
