package io.tapdata.pdk.tdd.core;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.entity.verification.ValueVerification;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.*;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.DataFlowEngine;
import io.tapdata.pdk.core.workflow.engine.TapDAG;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.*;

public class PDKTestBase {
    private static final String TAG = PDKTestBase.class.getSimpleName();
    protected TapConnector testConnector;
    protected TapConnector tddConnector;
    protected File testConfigFile;
    protected File jarFile;

    protected DataMap connectionOptions;
    protected DataMap nodeOptions;
    protected DataMap testOptions;

    private final AtomicBoolean completed = new AtomicBoolean(false);
    private boolean finishSuccessfully = false;
    private Throwable lastThrowable;

    protected TapDAG dag;

    public PDKTestBase() {
        String testConfig = CommonUtils.getProperty("pdk_test_config_file", "");
        testConfigFile = new File(testConfig);
        if (!testConfigFile.isFile())
            throw new IllegalArgumentException("TDD test config file doesn't exist or not a file, please check " + testConfigFile);

        String jarUrl = CommonUtils.getProperty("pdk_test_jar_file", "");
        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "./dist") + "/tdd-connector-v1.0-SNAPSHOT.jar";
        File tddJarFile = new File(tddJarUrl);
        if (!tddJarFile.isFile())
            throw new IllegalArgumentException("TDD jar file doesn't exist or not a file, please check " + tddJarFile);

        if (StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        jarFile = new File(jarUrl);
        if (!jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarUrl + " is not a file or not exists");
        TapConnectorManager.getInstance().start(Arrays.asList(jarFile, tddJarFile));
        testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
            String iconPath = specification.getIcon();
            TapLogger.info(TAG, "Found connector name {} id {} group {} version {} icon {}", specification.getName(), specification.getId(), specification.getGroup(), specification.getVersion(), specification.getIcon());
            if (StringUtils.isNotBlank(iconPath)) {
                InputStream is = nodeInfo.readResource(iconPath);
                if (is == null) {
                    TapLogger.error(TAG, "Icon image file doesn't be found for url {} which defined in spec json file.");
                }
            }
        }

        tddConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(tddJarFile.getName());
        PDKInvocationMonitor.getInstance().setErrorListener(errorMessage -> {
            if(enterWaitCompletedStage.get()) {
                $(() -> {
                    try {
                        fail(errorMessage);
                    } finally {
                        tearDown();
                    }
                });
            } else {
                fail(errorMessage);
            }
        });
    }

    public String testTableName(String id) {
        return id.replace('-', '_').replace(" ", "").replace('>', '_') + "_" + RandomStringUtils.randomAlphabetic(6);
    }

    public interface AssertionCall {
        void assertIt() throws InvocationTargetException, IllegalAccessException;
    }

    public void $(AssertionCall assertionCall) {
        try {
            assertionCall.assertIt();
        } catch (Throwable throwable) {
            lastThrowable = throwable;
            completed(true);
        }
    }

    public static SupportFunction supportAny(List<Class<? extends TapFunction>> functions, String errorMessage) {
        return new SupportFunction(functions, errorMessage);
    }

    public static SupportFunction support(Class<? extends TapFunction> function, String errorMessage) {
        return new SupportFunction(function, errorMessage);
    }

    public void checkFunctions(ConnectorFunctions connectorFunctions, List<SupportFunction> functions) {
        for(SupportFunction supportFunction : functions) {
            try {
                if(!PDKTestBase.isSupportFunction(supportFunction, connectorFunctions)) {
                    $(() -> fail(supportFunction.getErrorMessage()));
                }
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
                $(() -> fail("Check function for " + supportFunction.getFunction().getSimpleName() + " failed, method not found for " + e.getMessage()));
            }
        }
    }

    public static boolean isSupportFunction(SupportFunction supportFunction, ConnectorFunctions connectorFunctions) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        switch (supportFunction.getType()) {
            case SupportFunction.TYPE_ANY:
                AtomicBoolean hasAny = new AtomicBoolean(false);
                for(Class<? extends TapFunction> func : supportFunction.getAnyOfFunctions()) {
                    final Method method = connectorFunctions.getClass().getDeclaredMethod("get" + func.getSimpleName());
                    CommonUtils.ignoreAnyError(() -> {
                        Object obj = method.invoke(connectorFunctions);
                        if(obj != null) {
                            hasAny.set(true);
                        }
                    }, TAG);
                    if(hasAny.get())
                        break;
                }
                return hasAny.get();
            case SupportFunction.TYPE_ONE:
                Method method = connectorFunctions.getClass().getDeclaredMethod("get" + supportFunction.getFunction().getSimpleName());
                return method.invoke(connectorFunctions) != null;
        }
        return false;
    }


    public void completed() {
        completed(false);
    }

    public void completed(boolean withError) {
        if (completed.compareAndSet(false, true)) {
            finishSuccessfully = !withError;
//            PDKLogger.enable(false);
            synchronized (completed) {
                completed.notifyAll();
            }
            if(withError) {
                synchronized (this) {
                    try {
                        this.wait(50000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                    throw new QuiteException("Exit on failed");
                }
            }
        }
    }

    private AtomicBoolean enterWaitCompletedStage = new AtomicBoolean(false);
    public void waitCompleted(long seconds) throws Throwable {
        while (!completed.get()) {
            synchronized (completed) {
                enterWaitCompletedStage.compareAndSet(false, true);
                if (!completed.get()) {
                    try {
                        completed.wait(seconds * 1000);
                        completed.set(true);
                        if (lastThrowable == null && !finishSuccessfully)
                            throw new TimeoutException("Waited " + seconds + " seconds and still not completed, consider timeout execution.");
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                        TapLogger.error(TAG, "Completed wait interrupted " + interruptedException.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        try {
            if (lastThrowable != null)
                throw lastThrowable;
        } finally {
            tearDown();
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    public Map<String, DataMap> readTestConfig(File testConfigFile) {
        String testConfigJson = null;
        try {
            testConfigJson = FileUtils.readFileToString(testConfigFile, "utf8");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CoreException(PDKRunnerErrorCodes.TDD_READ_TEST_CONFIG_FAILED, "Test config file " + testConfigJson + " read failed, " + e.getMessage());
        }
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        return jsonParser.fromJson(testConfigJson, new TypeHolder<Map<String, DataMap>>() {
        });
    }

    public void prepareConnectionNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectionNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createConnectionConnectorBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareSourceNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectorNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createConnectorBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareTargetNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectorNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createConnectorBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void prepareSourceAndTargetNode(TapNodeInfo nodeInfo, DataMap connection, Consumer<ConnectorNode> consumer) {
        try {
            consumer.accept(PDKIntegration.createConnectorBuilder()
                    .withPdkId(nodeInfo.getTapNodeSpecification().getId())
                    .withAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup())
                    .withGroup(nodeInfo.getTapNodeSpecification().getGroup())
                    .withVersion(nodeInfo.getTapNodeSpecification().getVersion())
                    .withConnectionConfig(connection)
                    .build());
        } finally {
            PDKIntegration.releaseAssociateId("associated_" + nodeInfo.getTapNodeSpecification().idAndGroup());
        }
    }

    public void consumeQualifiedTapNodeInfo(Consumer<TapNodeInfo> consumer) {
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(PDKRunnerErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + jarFile);

        String pdkId = null;
        if (testOptions != null) {
            pdkId = (String) testOptions.get("pdkId");
        }
        if (pdkId == null) {
            pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
            if (pdkId == null)
                fail("Test pdkId is not specified");
        }
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            if (nodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                consumer.accept(nodeInfo);
                break;
            }
        }
    }

    @BeforeAll
    public static void setupAll() {
        DataFlowEngine.getInstance().start();
    }

    @BeforeEach
    public void setup() {
        TapLogger.info(TAG, "************************{} setup************************", this.getClass().getSimpleName());
        Map<String, DataMap> testConfigMap = readTestConfig(testConfigFile);
        assertNotNull(testConfigMap, "testConfigFile " + testConfigFile + " read to json failed");
        connectionOptions = testConfigMap.get("connection");
        nodeOptions = testConfigMap.get("node");
        testOptions = testConfigMap.get("test");
    }

    @AfterEach
    public void tearDown() {
        if (dag != null) {
            if(DataFlowEngine.getInstance().stopDataFlow(dag.getId())) {
                TapLogger.info(TAG, "************************{} tearDown************************", this.getClass().getSimpleName());
            }
        }
    }

    public DataMap getTestOptions() {
        return testOptions;
    }

    protected boolean mapEquals(Map<String, Object> firstRecord, Map<String, Object> result, StringBuilder builder) {
//        return InstanceFactory.instance(ValueVerification.class).mapEquals(firstRecord, result, ValueVerification.EQUALS_TYPE_FUZZY);
        MapDifference<String, Object> difference = Maps.difference(firstRecord, result);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        builder.append("Differences: \n");
        boolean different = false;
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            MapDifference.ValueDifference<Object> diff = entry.getValue();
            Object leftValue = diff.leftValue();
            Object rightValue = diff.rightValue();

            boolean equalResult = objectIsEqual(leftValue, rightValue);

            if (!equalResult) {
                different = true;
                builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                builder.append("\t\t").append("Left ").append(diff.leftValue()).append(" class ").append(diff.leftValue().getClass().getSimpleName()).append("\n");
                builder.append("\t\t").append("Right ").append(diff.rightValue()).append(" class ").append(diff.rightValue().getClass().getSimpleName()).append("\n");
            }
        }
        Map<String, Object> onlyOnLeft = difference.entriesOnlyOnLeft();
        if(!onlyOnLeft.isEmpty()) {
            different = true;
            for(Map.Entry<String, Object> entry : onlyOnLeft.entrySet()) {
                builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                builder.append("\t\t").append("Left ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
                builder.append("\t\t").append("Right ").append("N/A").append("\n");
            }
        }
        //Allow more on right.
//        Map<String, Object> onlyOnRight = difference.entriesOnlyOnRight();
//        if(!onlyOnRight.isEmpty()) {
//            different = true;
//            for(Map.Entry<String, Object> entry : onlyOnRight.entrySet()) {
//                builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
//                builder.append("\t\t").append("Left ").append("N/A").append("\n");
//                builder.append("\t\t").append("Right ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
//            }
//        }
        return !different;
    }

    public boolean objectIsEqual(Object leftValue, Object rightValue) {
        boolean equalResult = false;
//        if ((leftValue instanceof List) && (rightValue instanceof List)) {
//            if (((List<?>) leftValue).size() == ((List<?>) rightValue).size()) {
//                for (int i = 0; i < ((List<?>) leftValue).size(); i++) {
//                    equalResult = objectIsEqual(((List<?>) leftValue).get(i), ((List<?>) rightValue).get(i));
//                    if (!equalResult) break;
//                }
//            }
//        }

        if ((leftValue instanceof byte[]) && (rightValue instanceof byte[])) {
            equalResult = Arrays.equals((byte[]) leftValue, (byte[]) rightValue);
        } else if ((leftValue instanceof byte[]) && (rightValue instanceof String)) {
            //byte[] vs string, base64 decode string
            try {
//                    byte[] rightBytes = Base64.getDecoder().decode((String) rightValue);
                byte[] rightBytes = Base64.decodeBase64((String) rightValue);
                equalResult = Arrays.equals((byte[]) leftValue, rightBytes);
            } catch (Throwable ignored) {
            }
        } else if ((leftValue instanceof Number) && (rightValue instanceof Number)) {
            //number vs number, equal by value
            BigDecimal leftB = null;
            BigDecimal rightB = null;
            if (leftValue instanceof BigDecimal) {
                leftB = (BigDecimal) leftValue;
            }
            if (rightValue instanceof BigDecimal) {
                rightB = (BigDecimal) rightValue;
            }
            if (leftB == null) {
                leftB = BigDecimal.valueOf(((Number) leftValue).doubleValue());
            }
            if (rightB == null) {
                rightB = BigDecimal.valueOf(((Number) rightValue).doubleValue());
            }
            equalResult = leftB.compareTo(rightB) == 0;
        } else if ((leftValue instanceof Boolean)) {
            if (rightValue instanceof Number) {
                //boolean true == (!=0), false == 0
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((Number) rightValue).longValue() != 0;
                } else {
                    equalResult = ((Number) rightValue).longValue() == 0;
                }
            } else if (rightValue instanceof String) {
                //boolean true == "true", false == "false"
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((String) rightValue).equalsIgnoreCase("true");
                } else {
                    equalResult = ((String) rightValue).equalsIgnoreCase("false");
                }
            }
        }else{
            equalResult = leftValue.equals(rightValue);
        }
        return equalResult;
    }

    public DataMap buildInsertRecord() {
        DataMap insertRecord = new DataMap();
        insertRecord.put("id", "id_2");
        insertRecord.put("tapString", "1234");
        insertRecord.put("tapString10", "0987654321");
        insertRecord.put("tapInt", 123123);
        insertRecord.put("tapBoolean", true);
        insertRecord.put("tapNumber", 123.0);
        insertRecord.put("tapNumber52", 343.22);
        insertRecord.put("tapBinary", new byte[]{123, 21, 3, 2});
        return insertRecord;
    }

    public DataMap buildFilterMap() {
        DataMap filterMap = new DataMap();
        filterMap.put("id", "id_2");
        filterMap.put("tapString", "1234");
        return filterMap;
    }

    public DataMap buildUpdateMap() {
        DataMap updateMap = new DataMap();
        updateMap.put("id", "id_2");
        updateMap.put("tapString", "1234");
        updateMap.put("tapInt", 5555);
        return updateMap;
    }

    public void sendInsertRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTable, DataMap after) {
        sendInsertRecordEvent(dataFlowEngine, dag, sourceTable, after, null);
    }

    public void sendInsertRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTable, DataMap after, PatrolEvent patrolEvent) {
//        TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
//        tapInsertRecordEvent.setAfter(after);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), insertRecordEvent(after, sourceTable));
        if(patrolEvent != null)
            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendPatrolEvent(DataFlowEngine dataFlowEngine, TapDAG dag, PatrolEvent patrolEvent) {
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendUpdateRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTableId, DataMap before, DataMap after, PatrolEvent patrolEvent) {
//        TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
//        tapUpdateRecordEvent.setAfter(after);
//        tapUpdateRecordEvent.setBefore(before);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), updateDMLEvent(before, after, sourceTableId));
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendDeleteRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTableId, DataMap before, PatrolEvent patrolEvent) {
        TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent();
        tapDeleteRecordEvent.setBefore(before);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), deleteDMLEvent(before, sourceTableId));
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendCreateTableEvent(DataFlowEngine dataFlowEngine, TapDAG dag, TapTable table, PatrolEvent patrolEvent) {
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
        createTableEvent.setTable(table);
        createTableEvent.setTableId(table.getId());
        dataFlowEngine.sendExternalTapEvent(dag.getId(), createTableEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendDropTableEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String tableId, PatrolEvent patrolEvent) {
        TapDropTableEvent tapDropTableEvent = new TapDropTableEvent();
        tapDropTableEvent.setTableId(tableId);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), tapDropTableEvent);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    protected void verifyUpdateOneRecord(ConnectorNode targetNode, DataMap before, DataMap verifyRecord) {
        TapFilter filter = new TapFilter();
        filter.setMatch(before);
//        filter.setTableId(targetNode.getTable());
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(before) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));

        $(() -> assertNotNull(filterResult.getResult().get("tapInt"), "The value of tapInt should not be null"));
        for (Map.Entry<String, Object> entry : verifyRecord.entrySet()) {
            $(() -> assertTrue(objectIsEqual(entry.getValue(), filterResult.getResult().get(entry.getKey())), "The value of \"" + entry.getKey() + "\" should be \"" + entry.getValue() + "\",but actual it is \"" + filterResult.getResult().get(entry.getKey()) + "\", please make sure TapUpdateRecordEvent is handled well in writeRecord method"));
        }
    }

    private FilterResult filterResults(ConnectorNode targetNode, TapFilter filter, TapTable targetTable) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        if(queryByFilterFunction != null) {
            List<FilterResult> results = new ArrayList<>();
            List<TapFilter> filters = Collections.singletonList(filter);
            CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, targetTable, results::addAll));
            if(results.size() > 0)
                return results.get(0);
        } else {
            QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = targetNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
            if(queryByAdvanceFilterFunction != null) {
                FilterResult filterResult = new FilterResult();
                CommonUtils.handleAnyError(() -> queryByAdvanceFilterFunction.query(targetNode.getConnectorContext(), TapAdvanceFilter.create().match(filter.getMatch()), targetTable, filterResults -> {
                    if(filterResults != null && filterResults.getResults() != null && !filterResults.getResults().isEmpty())
                        filterResult.setResult(filterResults.getResults().get(0));
                    else if(filterResults.getError() != null)
                        filterResult.setError(filterResults.getError());
                }));
                return filterResult;
            }
        }
        return null;
    }

//    protected Object getStreamOffset(SourceNode sourceNode, List<String> tableList, Long offsetStartTime) throws Throwable {
//        TimestampToStreamOffsetFunction queryByFilterFunction = sourceNode.getConnectorFunctions().getStreamOffsetFunction();
//        final Object[] offset = {null};
//        queryByFilterFunction.streamOffset(sourceNode.getConnectorContext(), tableList, offsetStartTime, (o, aLong) -> offset[0] = o);
//        return offset[0];
//    }

    protected long getBatchCount(ConnectorNode sourceNode, TapTable table) throws Throwable {
        BatchCountFunction batchCountFunction = sourceNode.getConnectorFunctions().getBatchCountFunction();
        return batchCountFunction.count(sourceNode.getConnectorContext(), table);
    }

    protected void verifyTableNotExists(ConnectorNode targetNode, DataMap filterMap) {
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
//        filter.setTableId(targetNode.getTable());
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));

        if(filterResult.getResult() == null) {
            $(() -> Assertions.assertNull(filterResult.getResult(), "Table does not exist, result should be null"));
        } else {
            $(() -> assertNotNull(filterResult.getError(), "Table does not exist, error should be threw"));
        }
    }

    protected void verifyRecordNotExists(ConnectorNode targetNode, DataMap filterMap) {
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
//        filter.setTableId(targetNode.getTable());
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));

        Object result = filterResult.getResult();
        $(() -> Assertions.assertNull(filterResult.getResult(), "Result should be null, as the record has been deleted, please make sure TapDeleteRecordEvent is handled well in writeRecord method."));
        if (result != null) {
            $(() -> assertNotNull(filterResult.getError(), "If table not exist case, an error should be throw, otherwise not correct. "));
        }
    }

    protected void verifyBatchRecordExists(ConnectorNode sourceNode, ConnectorNode targetNode, DataMap filterMap) {
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
        TapTable sourceTable = sourceNode.getConnectorContext().getTableMap().get(sourceNode.getTable());
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));

        $(() -> Assertions.assertNull(filterResult.getError(), "Error occurred while queryByFilter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " error " + filterResult.getError()));
        $(() -> assertNotNull(filterResult.getResult(), "Result should not be null, as the record has been inserted"));
        Map<String, Object> result = filterResult.getResult();

        targetNode.getCodecsFilterManager().transformToTapValueMap(result, sourceTable.getNameFieldMap());
        targetNode.getCodecsFilterManager().transformFromTapValueMap(result);

        StringBuilder builder = new StringBuilder();
        $(() -> assertTrue(mapEquals(buildInsertRecord(), result, builder), builder.toString()));
    }

    public TapConnector getTestConnector() {
        return testConnector;
    }

    public File getTestConfigFile() {
        return testConfigFile;
    }

    public File getJarFile() {
        return jarFile;
    }

    public DataMap getConnectionOptions() {
        return connectionOptions;
    }

    public DataMap getNodeOptions() {
        return nodeOptions;
    }
}
