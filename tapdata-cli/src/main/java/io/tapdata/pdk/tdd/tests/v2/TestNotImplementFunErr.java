package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;


@DisplayName("Test.TestNotImplementFunErr")
@TapGo(tag = "V2", sort = 990, goTest = false)
public class TestNotImplementFunErr extends PDKTestBase {
    private static final String TAG = TestNotImplementFunErr.class.getSimpleName();
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("TestNotImplementFunErr.test.wait"));
        }
    }
    protected TapTable targetTable = table(testTableId)
            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
            .add(field("name", "STRING"))
            .add(field("text", "STRING"));

    @Test
    @DisplayName("Test.TestNotImplementFunErr.case.sourceTest")
    @TapTestCase(sort = 1)
    void sourceTest() throws Throwable {
        System.out.println(LangUtil.format("sourceTest.test.wait"));
        consumeQualifiedTapNodeInfo(nodeInfo -> {
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
            kvMap.put(testTableId, targetTable);
            ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
                    .withDagId(dagId)
                    .withLog(new TapLog())
                    .withAssociateId(UUID.randomUUID().toString())
                    .withConnectionConfig(connectionOptions)
                    .withGroup(spec.getGroup())
                    .withVersion(spec.getVersion())
                    .withTableMap(kvMap)
                    .withPdkId(spec.getId())
                    .withGlobalStateMap(stateMap)
                    .withStateMap(stateMap)
                    .withLog(new TapLog())
                    .withTable(testTableId)
                    .build();

            TapConnectorContext connectionContext = connectorNode.getConnectorContext(); //new TapConnectorContext(spec, connectionOptions, new DataMap(), connectorNode.getConnectorContext().getLog());

            try {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, "Init PDK", "TEST mongodb");
                writeRecorde(connectionContext, connectorNode, this.getMethod("sourceTest"));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (null != connectorNode) {
                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, "Stop PDK", "TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
        //waitCompleted(5000000);
    }


    private void writeRecorde(TapConnectorContext connectionContext, ConnectorNode connectorNode, Method testCase) throws Throwable {

    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(CreateTableFunction.class, LangUtil.format(inNeedFunFormat, "CreateTableFunction")),
                support(DropTableFunction.class, LangUtil.format(inNeedFunFormat, "DropTableFunction"))
        );
    }
}
