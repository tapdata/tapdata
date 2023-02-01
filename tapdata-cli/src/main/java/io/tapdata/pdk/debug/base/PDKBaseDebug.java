package io.tapdata.pdk.debug.base;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PDKBaseDebug extends PDKTestBase {
    private static final String TAG = PDKBaseDebug.class.getSimpleName();
    protected LinkedHashMap<String, Object> debugConfig = new LinkedHashMap<>();

    public PDKBaseDebug() {
        super();
        Map<String, DataMap> testConfigMap = readTestConfig(super.testConfigFile);
        assertNotNull(testConfigMap, "testConfigFile " + super.testConfigFile + " read to json failed");
        this.debugConfig = Optional.ofNullable(testConfigMap.get("function_params")).orElse(new DataMap());
    }

    protected TestNode prepare(TapNodeInfo nodeInfo){
        Map<String, DataMap> testConfigMap = readTestConfig(super.testConfigFile);
        assertNotNull(testConfigMap, "testConfigFile " + super.testConfigFile + " read to json failed");
        super.connectionOptions = Optional.ofNullable(testConfigMap.get("connectionConfig")).orElse(new DataMap());
        super.nodeOptions = Optional.ofNullable(testConfigMap.get("nodeConfig")).orElse(new DataMap());

        tapNodeInfo = nodeInfo;
        originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();
        testTableId = tableNameCreator.tableName();
        targetTable.setId(testTableId);
        KVMap<Object> stateMap = new KVMap<Object>() {
            @Override
            public void init(String mapKey, Class<Object> valueClass) {

            }

            @Override
            public void put(String key, Object o) {

            }

            @Override
            public Object putIfAbsent(String key, Object o) {
                return null;
            }

            @Override
            public Object remove(String key) {
                return null;
            }

            @Override
            public void clear() {

            }

            @Override
            public void reset() {

            }

            @Override
            public Object get(String key) {
                return null;
            }
        };
        String dagId = UUID.randomUUID().toString();
        KVMap<TapTable> kvMap = InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, TapTable.class);
        TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
        kvMap.put(testTableId,targetTable);
        ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
                .withDagId(dagId)
                .withAssociateId(UUID.randomUUID().toString())
                .withConnectionConfig(connectionOptions)
                .withNodeConfig(nodeOptions)
                .withGroup(spec.getGroup())
                .withVersion(spec.getVersion())
                .withTableMap(kvMap)
                .withPdkId(spec.getId())
                .withGlobalStateMap(stateMap)
                .withStateMap(stateMap)
                .withTable(testTableId)
                .build();
        RecordEventExecute recordEventExecute = RecordEventExecute.create(connectorNode, this);
        return new TestNode( connectorNode, recordEventExecute);
    }
}
