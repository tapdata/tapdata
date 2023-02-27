package io.tapdata.pdk.tdd.core;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.error.QuiteException;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.DataFlowEngine;
import io.tapdata.pdk.core.workflow.engine.DataFlowWorker;
import io.tapdata.pdk.core.workflow.engine.TapDAG;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.tdd.tests.support.connector.TableNameSupport;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

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
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;
import static org.junit.jupiter.api.Assertions.*;

public class PDKTestBase {
    public static final String inNeedFunFormat = "function.inNeed";
    public static final String anyOneFunFormat = "functions.anyOneNeed";

    private static final String TAG = PDKTestBase.class.getSimpleName();
    protected TapConnector testConnector;

    public TapConnector testConnector() {
        return this.testConnector;
    }

    protected TapConnector tddConnector;
    protected File testConfigFile;
    protected File jarFile;

    protected String testNodeId;
    protected TableNameSupport tableNameCreator;

    protected DataMap connectionOptions;
    protected DataMap nodeOptions;
    protected DataMap testOptions;

    private final AtomicBoolean completed = new AtomicBoolean(false);
    private boolean finishSuccessfully = false;
    private Throwable lastThrowable;

    protected TapDAG dag;

    protected String lang;

    private void tableNameCreator(Collection<TapNodeInfo> tapNodeInfoCollection) {
        if (tapNodeInfoCollection.isEmpty()) {
            this.tableNameCreator = TableNameSupport.support(null);
            return;
        }
        tapNodeInfoCollection.stream().filter(Objects::nonNull).forEach((nodeInfo) -> {
            this.testNodeId = nodeInfo.getTapNodeSpecification().getId();
            this.tableNameCreator = TableNameSupport.support(this.testNodeId);
            return;
        });
        if (Objects.isNull(this.tableNameCreator)) this.tableNameCreator = TableNameSupport.support(null);
    }

    protected void connectorOnStart(TestNode prepare) {
        try {
            PDKInvocationMonitor.invoke(prepare.connectorNode(),
                    PDKMethod.INIT,
                    prepare.connectorNode()::connectorInit,
                    "Init PDK", this.testNodeId + " connector"
            );
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    protected void connectorOnStop(TestNode prepare) {
        if (null != prepare.connectorNode()) {
            PDKInvocationMonitor.invoke(prepare.connectorNode(),
                    PDKMethod.STOP,
                    prepare.connectorNode()::connectorStop,
                    "Stop PDK",
                    this.testNodeId + "  connector"
            );
            PDKIntegration.releaseAssociateId("releaseAssociateId");
        }
    }
//    protected Map<Class, CapabilitiesExecutionMsg> capabilitiesResult = new HashMap<>();

    public PDKTestBase() {
        String testConfig = CommonUtils.getProperty("pdk_test_config_file", "");
        this.testConfigFile = new File(testConfig);
        if (!this.testConfigFile.isFile())
            throw new IllegalArgumentException("TDD test config file doesn't exist or not a file, please check " + this.testConfigFile);

        String jarUrl = CommonUtils.getProperty("pdk_test_jar_file", "");
        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "../connectors/dist") + "/tdd-connector-v1.0-SNAPSHOT.jar";
        File tddJarFile = new File(tddJarUrl);
        if (!tddJarFile.isFile())
            throw new IllegalArgumentException("TDD jar file doesn't exist or not a file, please check " + tddJarFile.getAbsolutePath());

        if (StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        this.jarFile = new File(jarUrl);
        if (!this.jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + this.jarFile.getAbsolutePath() + " is not a file or not exists");
//        TapConnectorManager.getInstance().start();
        TapConnectorManager.getInstance().start(Arrays.asList(this.jarFile, tddJarFile));
        this.testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(this.jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = this.testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
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

        this.tddConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(tddJarFile.getName());
        this.tableNameCreator(tapNodeInfoCollection);

        PDKInvocationMonitor.getInstance().setErrorListener(errorMessage -> {
            if (this.enterWaitCompletedStage.get()) {
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

    public static SupportFunction supportAny(List<Class<? extends TapFunction>> functions, String errorMessage) {
        return new SupportFunction(functions, errorMessage);
    }

    @SafeVarargs
    public static SupportFunction supportAny(String errorMessage, Class<? extends TapFunction>... classes) {
        List<Class<? extends TapFunction>> functions = new ArrayList<>();
        if (Objects.nonNull(classes) && classes.length > 0) {
            functions.addAll(Arrays.asList(classes));
        }
        return new SupportFunction(functions, Optional.ofNullable(errorMessage).orElse(""));
    }

    public static SupportFunction support(Class<? extends TapFunction> function, String errorMessage) {
        return new SupportFunction(function, errorMessage);
    }

    public void checkFunctions(ConnectorFunctions connectorFunctions, List<SupportFunction> functions) {
        for (SupportFunction supportFunction : functions) {
            try {
                if (!PDKTestBase.isSupportFunction(supportFunction, connectorFunctions)) {
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
                for (Class<? extends TapFunction> func : supportFunction.getAnyOfFunctions()) {
                    final Method method = connectorFunctions.getClass().getMethod("get" + func.getSimpleName());
                    CommonUtils.ignoreAnyError(() -> {
                        Object obj = method.invoke(connectorFunctions);
                        if (obj != null) {
                            hasAny.set(true);
                        }
                    }, TAG);
                    if (hasAny.get())
                        break;
                }
                return hasAny.get();
            case SupportFunction.TYPE_ONE:
                Method method = connectorFunctions.getClass().getMethod("get" + supportFunction.getFunction().getSimpleName());
                return method.invoke(connectorFunctions) != null;
        }
        return false;
    }

    protected boolean verifyFunctions(ConnectorFunctions functions, Method testCase) {
        if (Objects.isNull(functions)) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("notFunctions"))).error(testCase);
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public interface AssertionCall {
        void assertIt() throws InvocationTargetException, IllegalAccessException;
    }

    public void $(AssertionCall assertionCall) {
        try {
            assertionCall.assertIt();
        } catch (Throwable throwable) {
            this.lastThrowable = throwable;
            completed(true);
        }
    }

    public void completed() {
        completed(false);
    }

    public void completed(boolean withError) {
        if (this.completed.compareAndSet(false, true)) {
            this.finishSuccessfully = !withError;
            //PDKLogger.enable(false);
            synchronized (this.completed) {
                this.completed.notifyAll();
            }
            if (withError) {
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
        while (!this.completed.get()) {
            synchronized (this.completed) {
                this.enterWaitCompletedStage.compareAndSet(false, true);
                if (!this.completed.get()) {
                    try {
                        this.completed.wait(seconds * 1000);
                        this.completed.set(true);
                        if (this.lastThrowable == null && !this.finishSuccessfully)
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
            if (this.lastThrowable != null)
                throw this.lastThrowable;
        } finally {
            this.tearDown();
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
                    .withLog(new TapLog())
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
        Collection<TapNodeInfo> tapNodeInfoCollection = this.testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(PDKRunnerErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + this.jarFile);
        String pdkId = null;
        if (this.testOptions != null) {
            pdkId = (String) this.testOptions.get("pdkId");
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
        Map<String, DataMap> testConfigMap = readTestConfig(this.testConfigFile);
        assertNotNull(testConfigMap, "testConfigFile " + this.testConfigFile + " read to json failed");
        this.connectionOptions = Optional.ofNullable(testConfigMap.get("connection")).orElse(new DataMap());
        this.nodeOptions = Optional.ofNullable(testConfigMap.get("node")).orElse(new DataMap());
        this.testOptions = Optional.ofNullable(testConfigMap.get("test")).orElse(new DataMap());
    }

    @AfterEach
    public void tearDown() {
        Optional.ofNullable(this.dag).ifPresent(d -> {
            if (DataFlowEngine.getInstance().stopDataFlow(this.dag.getId())) {
                TapLogger.info(TAG, "************************{} tearDown************************", this.getClass().getSimpleName());
            }
        });
    }

    public DataMap getTestOptions() {
        return this.testOptions;
    }

    protected boolean mapEquals(Map<String, Object> firstRecord, Map<String, Object> result, StringBuilder builder) {
        MapDifference<String, Object> difference = Maps.difference(firstRecord, result);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        builder.append("\t\t\tDifferences: \n");
        boolean different = false;
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            MapDifference.ValueDifference<Object> diff = entry.getValue();
            Object leftValue = diff.leftValue();
            Object rightValue = diff.rightValue();
            boolean equalResult = objectIsEqual(leftValue, rightValue);
            if (!equalResult) {
                different = true;
                builder.append("\t\t\t\t").append("Key ").append(entry.getKey()).append("\n");
                Object valueObj = diff.leftValue();
                String valueClassName = Objects.isNull(valueObj) ? "" : valueObj.getClass().getSimpleName();
                builder.append("\t\t\t\t\t").append("Left ").append(diff.leftValue()).append(" class ").append(valueClassName).append("\n");
                builder.append("\t\t\t\t\t").append("Right ").append(diff.rightValue()).append(" class ").append(valueClassName).append("\n");
            }
        }
        Map<String, Object> onlyOnLeft = difference.entriesOnlyOnLeft();
        if (!onlyOnLeft.isEmpty()) {
            different = true;
            for (Map.Entry<String, Object> entry : onlyOnLeft.entrySet()) {
                builder.append("\t\t\t\t").append("Key ").append(entry.getKey()).append("\n");
                builder.append("\t\t\t\t\t").append("Left ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
                builder.append("\t\t\t\t\t").append("Right ").append("N/A").append("\n");
            }
        }
        //Allow more on right.
        //Map<String, Object> onlyOnRight = difference.entriesOnlyOnRight();
        //if(!onlyOnRight.isEmpty()) {
        //    different = true;
        //    for(Map.Entry<String, Object> entry : onlyOnRight.entrySet()) {
        //        builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
        //        builder.append("\t\t").append("Left ").append("N/A").append("\n");
        //        builder.append("\t\t").append("Right ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
        //    }
        //}
        return !different;
    }

    public boolean objectIsEqual(Object leftValue, Object rightValue) {
        boolean equalResult = false;
        //if ((leftValue instanceof List) && (rightValue instanceof List)) {
        //    if (((List<?>) leftValue).size() == ((List<?>) rightValue).size()) {
        //        for (int i = 0; i < ((List<?>) leftValue).size(); i++) {
        //            equalResult = objectIsEqual(((List<?>) leftValue).get(i), ((List<?>) rightValue).get(i));
        //            if (!equalResult) break;
        //        }
        //    }
        //}
        if ((leftValue instanceof byte[]) && (rightValue instanceof byte[])) {
            equalResult = Arrays.equals((byte[]) leftValue, (byte[]) rightValue);
        } else if ((leftValue instanceof byte[]) && (rightValue instanceof String)) {
            //byte[] vs string, base64 decode string
            try {
                //byte[] rightBytes = Base64.getDecoder().decode((String) rightValue);
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
            if (Objects.isNull(leftB)) {
                leftB = BigDecimal.valueOf(Double.parseDouble(String.valueOf((leftValue))));
            }
            if (Objects.isNull(rightB)) {
                rightB = BigDecimal.valueOf(Double.parseDouble(String.valueOf((rightValue))));
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
        } else if (rightValue instanceof Date) {
            Date date = (Date) rightValue;
            String dataStr = DateUtil.dateTimeToStr(date);
            equalResult = dataStr.contains(String.valueOf(leftValue));
        } else {
            equalResult = leftValue.equals(rightValue);
        }
        return equalResult;
    }

    public DataMap buildInsertRecord() {
        DataMap insertRecord = new DataMap();
        insertRecord.put("id", "id_2");
        insertRecord.put("tap_string", "1234");
        insertRecord.put("tap_string10", "0987654321");
        insertRecord.put("tap_int", 123123);
        insertRecord.put("tap_boolean", true);
        insertRecord.put("tap_number", 123.0);
        insertRecord.put("tap_number52", 343.22);
        insertRecord.put("tap_binary", new byte[]{123, 21, 3, 2});
        return insertRecord;
    }

    public DataMap buildFilterMap() {
        DataMap filterMap = new DataMap();
        filterMap.put("id", "id_2");
        filterMap.put("tap_string", "1234");
        return filterMap;
    }

    public DataMap buildUpdateMap() {
        DataMap updateMap = new DataMap();
        updateMap.put("id", "id_2");
        updateMap.put("tap_string", "1234");
        updateMap.put("tap_int", 5555);
        return updateMap;
    }

    public void sendInsertRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTable, DataMap after) {
        sendInsertRecordEvent(dataFlowEngine, dag, sourceTable, after, null);
    }

    public void sendInsertRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTable, DataMap after, PatrolEvent patrolEvent) {
        //TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
        //tapInsertRecordEvent.setAfter(after);
        dataFlowEngine.sendExternalTapEvent(dag.getId(), insertRecordEvent(after, sourceTable));
        if (patrolEvent != null)
            dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendPatrolEvent(DataFlowEngine dataFlowEngine, TapDAG dag, PatrolEvent patrolEvent) {
        dataFlowEngine.sendExternalTapEvent(dag.getId(), patrolEvent);
    }

    public void sendUpdateRecordEvent(DataFlowEngine dataFlowEngine, TapDAG dag, String sourceTableId, DataMap before, DataMap after, PatrolEvent patrolEvent) {
        //TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
        //tapUpdateRecordEvent.setAfter(after);
        //tapUpdateRecordEvent.setBefore(before);
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
        //filter.setTableId(targetNode.getTable());
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());
        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(before) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));
        $(() -> assertNotNull(filterResult.getResult().get("tap_int"), "The value of tapInt should not be null"));
        for (Map.Entry<String, Object> entry : verifyRecord.entrySet()) {
            $(() -> assertTrue(objectIsEqual(entry.getValue(), filterResult.getResult().get(entry.getKey())), "The value of \"" + entry.getKey() + "\" should be \"" + entry.getValue() + "\",but actual it is \"" + filterResult.getResult().get(entry.getKey()) + "\", please make sure TapUpdateRecordEvent is handled well in writeRecord method"));
        }
    }

    protected FilterResult filterResults(ConnectorNode targetNode, TapFilter filter, TapTable targetTable) {
        QueryByFilterFunction queryByFilterFunction = targetNode.getConnectorFunctions().getQueryByFilterFunction();
        if (Objects.nonNull(queryByFilterFunction)) {
            List<FilterResult> results = new ArrayList<>();
            List<TapFilter> filters = Collections.singletonList(filter);
            CommonUtils.handleAnyError(() -> queryByFilterFunction.query(targetNode.getConnectorContext(), filters, targetTable, results::addAll));
            if (results.size() > 0)
                return results.get(0);
        } else {
            QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = targetNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
            if (queryByAdvanceFilterFunction != null) {
                FilterResult filterResult = new FilterResult();
                CommonUtils.handleAnyError(() -> queryByAdvanceFilterFunction.query(targetNode.getConnectorContext(), TapAdvanceFilter.create().match(filter.getMatch()), targetTable, filterResults -> {
                    if (filterResults != null && filterResults.getResults() != null && !filterResults.getResults().isEmpty())
                        filterResult.setResult(filterResults.getResults().get(0));
                    else if (filterResults.getError() != null)
                        filterResult.setError(filterResults.getError());
                }));
                return filterResult;
            }
        }
        return null;
    }

    protected long getBatchCount(ConnectorNode sourceNode, TapTable table) throws Throwable {
        BatchCountFunction batchCountFunction = sourceNode.getConnectorFunctions().getBatchCountFunction();
        return batchCountFunction.count(sourceNode.getConnectorContext(), table);
    }

    protected void verifyTableNotExists(ConnectorNode targetNode, DataMap filterMap) {
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        FilterResult filterResult = filterResults(targetNode, filter, targetTable);
        $(() -> assertNotNull(filterResult, "The filter " + InstanceFactory.instance(JsonParser.class).toJson(filterMap) + " can not get any result. Please make sure writeRecord method update record correctly and queryByFilter/queryByAdvanceFilter can query it out for verification. "));

        if (filterResult.getResult() == null) {
            $(() -> Assertions.assertNull(filterResult.getResult(), "Table does not exist, result should be null"));
        } else {
            $(() -> assertNotNull(filterResult.getError(), "Table does not exist, error should be threw"));
        }
    }

    protected void verifyRecordNotExists(ConnectorNode targetNode, DataMap filterMap) {
        TapFilter filter = new TapFilter();
        filter.setMatch(filterMap);
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

    public void lang(String lang) {
        this.lang = lang;
    }

    public Method getMethod(String methodName) throws NoSuchMethodException {
        return get().getDeclaredMethod(methodName);
    }

    public Class<? extends PDKTestBase> get() {
        return this.getClass();
    }

    private void setInsertPolicy(TapConnectorContext context, String policyName, String policy) {
        if (null == context) return;
        ConnectorCapabilities connectorCapabilities = context.getConnectorCapabilities();
        Map<String, String> capabilityAlternativeMap = connectorCapabilities.getCapabilityAlternativeMap();
        if (null == capabilityAlternativeMap) {
            capabilityAlternativeMap = new HashMap<>();
            connectorCapabilities.setCapabilityAlternativeMap(capabilityAlternativeMap);
        }
        capabilityAlternativeMap.put(policy, policy);
    }

    protected final String INSERT_POLICY = "dml_insert_policy";
    protected final String IGNORE_ON_EXISTS = "ignore_on_exists";
    protected final String UPDATE_ON_EXISTS = "update_on_exists";

    public void ignoreOnExistsWhenInsert(TapConnectorContext context) {
        setInsertPolicy(context, INSERT_POLICY, IGNORE_ON_EXISTS);
    }

    public void updateOnExistsWhenInsert(TapConnectorContext context) {
        setInsertPolicy(context, INSERT_POLICY, UPDATE_ON_EXISTS);
    }

    protected final String UPDATE_POLICY = "dml_update_policy";
    protected final String IGNORE_ON_NOT_EXISTS = "ignore_on_nonexists";
    protected final String INSERT_ON_NOT_EXISTS = "insert_on_nonexists";

    public void ignoreOnExistsWhenUpdate(TapConnectorContext context) {
        setInsertPolicy(context, UPDATE_POLICY, IGNORE_ON_NOT_EXISTS);
    }

    public void insertOnExistsWhenUpdate(TapConnectorContext context) {
        setInsertPolicy(context, UPDATE_POLICY, INSERT_ON_NOT_EXISTS);
    }


    protected ConnectorNode tddTargetNode;
    protected ConnectorNode sourceNode;
    protected DataFlowWorker dataFlowWorker;
    protected String targetNodeId = "t2";
    protected String testSourceNodeId = "ts1";
    protected String originToSourceId;
    protected TapNodeInfo tapNodeInfo;
    protected String testTableId;
    protected TapTable targetTable = table(testTableId)
            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
            //.add(field("TYPE_ARRAY", JAVA_Array).tapType(tapArray()))
            .add(field("TYPE_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
            .add(field("TYPE_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
            .add(field("TYPE_DATE", JAVA_Date).tapType(tapDate()))
            .add(field("TYPE_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
            //.add(field("TYPE_MAP", JAVA_Map).tapType(tapMap()))
            .add(field("TYPE_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
            .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
            .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(55).fixed(true)))
            .add(field("TYPE_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).precision(200).scale(55).fixed(false)))
            .add(field("TYPE_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(55).fixed(false)))
            .add(field("TYPE_STRING_1", JAVA_String).tapType(tapString().bytes(50L)))
            .add(field("TYPE_STRING_2", JAVA_String).tapType(tapString().bytes(50L)))
            .add(field("TYPE_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
            .add(field("TYPE_TIME", "Time").tapType(tapTime().withTimeZone(false)))
            .add(field("TYPE_YEAR", "Year").tapType(tapYear()));


    protected TestNode prepare(TapNodeInfo nodeInfo) {
        this.tapNodeInfo = nodeInfo;
        this.originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();
        this.testTableId = this.tableNameCreator.tableName();
        this.targetTable.setId(this.testTableId);
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
        kvMap.put(this.testTableId, this.targetTable);
        ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
                .withDagId(dagId)
                .withAssociateId(UUID.randomUUID().toString())
                .withConnectionConfig(this.connectionOptions)
                .withNodeConfig(this.nodeOptions)
                .withGroup(spec.getGroup())
                .withVersion(spec.getVersion())
                .withTableMap(kvMap)
                .withPdkId(spec.getId())
                .withGlobalStateMap(stateMap)
                .withStateMap(stateMap)
                .withTable(this.testTableId)
                .build();
        RecordEventExecute recordEventExecute = RecordEventExecute.create(connectorNode, this);
        return new TestNode(nodeInfo, connectorNode, recordEventExecute);
    }

    protected void initConnectorFunctions() {
        this.tddTargetNode = this.dataFlowWorker.getTargetNodeDriver(this.targetNodeId).getTargetNode();
        this.sourceNode = this.dataFlowWorker.getSourceNodeDriver(this.testSourceNodeId).getSourceNode();
    }

    protected boolean createTable(TestNode prepare) {
        return this.createTable(prepare, null, Boolean.TRUE);
    }

    protected boolean createTable(TestNode prepare, boolean deleteRecordAfterCreateTable) {
        return this.createTable(prepare, null, deleteRecordAfterCreateTable);
    }

    protected boolean createTable(TestNode prepare, TapTable createTable) {
        return this.createTable(prepare, createTable, Boolean.TRUE);
    }

    protected void createTableFailedPrint(Method testCase, TapTable tapTable, TapAssertImpl tapAssert) {
        tapAssert.accept(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.errorTable", this.tableInfo(tapTable)));
    }

    protected String tableInfo(TapTable tapTable) {
        StringJoiner joiner = new StringJoiner(",\n");
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        Optional.ofNullable(nameFieldMap).ifPresent(fields -> {
            fields.forEach((fieldName, field) -> {
                joiner.add(LangUtil.SPILT_GRADE_4 + " |\t[Field] " + fieldName + ": type( " +
                        field.getDataType() + " ) | nullable( " +
                        field.getNullable() + " ) ");
            });
        });
        return "\n" + LangUtil.SPILT_GRADE_4 + "Created table name: " + tapTable.getId() + "\n" + joiner.toString();
    }


    protected boolean createTable(TestNode prepare, TapTable createTable, boolean deleteRecordAfterCreateTable) {
        TapConnectorContext connectorContext = prepare.connectorNode().getConnectorContext();
        ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
        Method testCase = prepare.recordEventExecute().testCase();
        if (null != connectorFunctions) {
            CreateTableFunction createTableFunction = connectorFunctions.getCreateTableFunction();
            CreateTableV2Function createTableV2Function = connectorFunctions.getCreateTableV2Function();
            WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
            TapAssert asserts = TapAssert.asserts(() -> {
            });
            TapCreateTableEvent createTableEvent;
            if (Objects.isNull(createTable)) {
                this.targetTable.setId(this.tableNameCreator.tableName());
                this.targetTable.setName(this.targetTable.getId());
                createTableEvent = this.modelDeductionForCreateTableEvent(prepare.connectorNode());
            } else {
                createTableEvent = new TapCreateTableEvent();
                createTableEvent.table(createTable);
                createTableEvent.setReferenceTime(System.currentTimeMillis());
                createTableEvent.setTableId(createTable.getId());
            }
            //LinkedHashMap<String, TapField> modelDeduction = this.modelDeduction(prepare.connectorNode);
            //TapTable tabled = new TapTable();
            //modelDeduction.forEach((name,field)->{
            //    tabled.add(field);
            //});
            //tabled.setName(targetTable.getName());
            //tabled.setId(targetTable.getId());
            //createTableEvent.table(tabled);
            //createTableEvent.setReferenceTime(System.currentTimeMillis());
            //createTableEvent.setTableId(targetTable.getId());
            TapTable finalCreateTable = Optional.ofNullable(createTable).orElse(this.targetTable);
            String finalTableId = finalCreateTable.getId();
            if (null != createTableV2Function) {
                try {
                    CreateTableOptions table = createTableV2Function.createTable(connectorContext, createTableEvent);
                    asserts.acceptAsError(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.createTableV2Function.succeed", this.tableInfo(finalCreateTable)));
                    return this.verifyCreateTable(prepare, finalCreateTable);
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.createTableV2Function.error", finalTableId, this.tableInfo(finalCreateTable) + "\n\n" + LangUtil.SPILT_GRADE_4 + e.getMessage()));
                    //this.createTableFailedPrint(testCase, finalCreateTable, new TapAssertImpl(TapAssert.ERROR));
                }
            } else if (null != createTableFunction) {
                try {
                    createTableFunction.createTable(connectorContext, createTableEvent);
                    asserts.acceptAsError(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.createTableFunction.succeed", this.tableInfo(finalCreateTable)));
                    return this.verifyCreateTable(prepare, finalCreateTable);
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.createTableFunction.error", finalTableId, this.tableInfo(finalCreateTable) + "\n\n" + LangUtil.SPILT_GRADE_4 + e.getMessage()));
                    //this.createTableFailedPrint(testCase, finalCreateTable, new TapAssertImpl(TapAssert.ERROR));
                }
            } else if (null != writeRecordFunction) {
                Record[] records = Record.testRecordWithTapTable(finalCreateTable, 1);
                try {
                    WriteListResult<TapRecordEvent> insert = prepare.recordEventExecute()
                            .builderRecord(records)
                            .insert();
                    TapAssert.succeed(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.insertForCreateTable.succeed", records.length, finalTableId));
                    if (this.verifyCreateTable(prepare, finalCreateTable) && deleteRecordAfterCreateTable) {
                        prepare.recordEventExecute().delete();
                    }
                    return Boolean.TRUE;
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.insertForCreateTable.error", records.length, finalTableId, e.getMessage()));
                } finally {
                    prepare.recordEventExecute().resetRecords();
                }
            } else {
                String message = LangUtil.format("tableCount.findTableCountAfterNewTable.newTable.error");
                TapAssert.asserts(() -> Assertions.fail(message)).warn(testCase);
            }
        }
        return Boolean.FALSE;
    }

    private boolean verifyCreateTable(TestNode prepare) throws Throwable {
        return this.verifyCreateTable(prepare, null);
    }

    private boolean verifyCreateTable(TestNode prepare, TapTable verifyTable) throws Throwable {
        TapConnectorContext connectorContext = prepare.connectorNode().getConnectorContext();
        List<TapTable> tables = new ArrayList<>();
        String tableId = (Optional.ofNullable(verifyTable).orElse(this.targetTable)).getId();
        prepare.connectorNode().getConnector().discoverSchema(
                connectorContext, list(tableId), 1000, consumer -> Optional.ofNullable(consumer).ifPresent(tables::addAll)
        );
        TapAssert.asserts(() ->
                assertFalse(tables.isEmpty(), LangUtil.format("table.create.error", tableId))
        ).acceptAsError(prepare.recordEventExecute().testCase(), LangUtil.format("table.create.succeed", tableId));
        return !tables.isEmpty();
    }

    protected void checkIndex(Method testCase, List<TapIndex> ago, List<TapIndex> after) {
        if (null == ago || ago.isEmpty()) {
            return;
        }
        if (null == after || after.isEmpty()) {
            TapAssert.asserts(() ->
                    Assertions.fail(LangUtil.format("base.checkIndex.after.error", this.targetTable.getId()))
            ).error(testCase);
            return;
        }
        Map<String, List<TapIndexField>> collect = after.stream().collect(Collectors.toMap(
                TapIndex::getName,
                TapIndex::getIndexFields,
                (o1, o2) -> o1
        ));
        //未创建成功的字段
        StringJoiner notSucceedIndex = new StringJoiner(",");
        //创建了但是索引字段属性不相同
        StringJoiner notEqualsIndex = new StringJoiner(",");
        for (TapIndex indexItem : ago) {
            List<TapIndexField> indexFields = indexItem.getIndexFields();
            String indexName = indexItem.getName();

            List<TapIndexField> afterFields = collect.get(indexName);
            if (null == indexFields) {
                continue;
            }
            if (null == afterFields) {
                notSucceedIndex.add(indexName);
                continue;
            }
            StringJoiner agoIndexStr = new StringJoiner(",");
            StringJoiner afterIndexStr = new StringJoiner(",");

            Map<String, TapIndexField> afterFieldsMap = afterFields.stream().collect(Collectors.toMap(TapIndexField::getName, field -> field, (o1, o2) -> o1));
            for (TapIndexField indexField : afterFields) {
                afterIndexStr.add(indexField.getName() + "(" + indexField.getFieldAsc() + ")");
            }
            boolean notEquals = false;
            //索引字段数量不相等
            if (indexFields.size() != afterFields.size()) {
                notEquals = true;
            }
            for (TapIndexField indexField : indexFields) {
                String name = indexField.getName();
                if (!notEquals) {
                    TapIndexField afterField = afterFieldsMap.get(name);
                    if (null == afterField || afterField.getFieldAsc() != indexField.getFieldAsc()) {
                        notEquals = true;
                    }
                }
                agoIndexStr.add(name + "(" + indexField.getFieldAsc() + ")");
            }
            if (notEquals) {
                notEqualsIndex.add(indexName + "[(" + agoIndexStr.toString() + ")->(" + afterIndexStr.toString() + ")]");
            }
        }

        if (notSucceedIndex.length() > 0 || notEqualsIndex.length() > 0) {
            TapAssert.asserts(() ->
                    Assertions.fail(LangUtil.format(
                            "base.indexCreate.error",
                            notSucceedIndex.length() > 0 ? notSucceedIndex.toString() : "-",
                            notEqualsIndex.length() > 0 ? notEqualsIndex.toString() : "-",
                            targetTable.getId()
                            )
                    )
            ).warn(testCase);
        } else {
            TapAssert.succeed(testCase, LangUtil.format("base.succeed.createIndex", targetTable.getId()));
        }
    }

    protected void contrastTableFieldNameAndType(Method testCase, LinkedHashMap<String, TapField> sourceFields, LinkedHashMap<String, TapField> targetFields) {
        String tableId = this.targetTable.getId();
        if (null == sourceFields || sourceFields.isEmpty()) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.sourceFields.empty", tableId))).error(testCase);
            return;
        }
        if (null == targetFields || targetFields.isEmpty()) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.targetFields.empty", tableId))).error(testCase);
            return;
        }
        int sourceSize = sourceFields.size();
        int targetSize = targetFields.size();
        if (targetSize < sourceSize) {
            TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.targetSource.countNotEquals", sourceSize, targetSize, tableId))).error(testCase);
            return;
        }
        boolean inferSucceed = true;
        StringJoiner sourceBuilder = new StringJoiner(",");
        StringJoiner targetBuilder = new StringJoiner(",");
        for (Map.Entry<String, TapField> fieldEntry : sourceFields.entrySet()) {
            TapField field = fieldEntry.getValue();
            String name = field.getName();
            String type = field.getDataType();
            if (null == type || "".equals(type)) {
                //类型为空，推演失败
                TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.source.fieldDataType.null", name, tableId))).warn(testCase);
                return;
            }

            TapField targetField = targetFields.get(name);
            if (null == targetField) {
                TapAssert.asserts(() -> Assertions.fail(LangUtil.format("base.target.fieldDataType.null", tableId, name))).warn(testCase);
                return;
            }
            String targetType = targetField.getDataType();
            if (!type.equals(targetType)) {
                //类型不一样，建表不一致
                inferSucceed = false;
                sourceBuilder.add("(" + name + ":" + type + ")");
                targetBuilder.add("(" + name + ":" + targetType + ")");
            }
        }

        boolean finalInferSucceed = inferSucceed;
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        finalInferSucceed,
                        LangUtil.format("base.field.contrast.error", sourceBuilder.toString(), targetBuilder.toString(), tableId)
                )
        ).acceptAsWarn(testCase, LangUtil.format("base.field.contrast.succeed", tableId));
    }


    //模型推演
    protected LinkedHashMap<String, TapField> modelDeduction(ConnectorNode connectorNode) {
        //进行模型推演获取表结构
        //使用TapType的11种类型组织表结构（类型的长度尽量短小）
        TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        if (targetTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TARGET_TYPES_GENERATOR_NOT_FOUND, "TargetTypesGenerator's implementation is not found in current classloader");
        TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        if (tableFieldTypesGenerator == null)
            throw new CoreException(PDKRunnerErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");
        TapCodecsRegistry codecRegistry = connectorNode.getCodecsRegistry();
        TapCodecsFilterManager targetCodecFilterManager = connectorNode.getCodecsFilterManager();
        DefaultExpressionMatchingMap dataTypesMap = connectorNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap();
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(
                this.targetTable.getNameFieldMap(),
                dataTypesMap,
                targetCodecFilterManager
        );
        //经过模型推演生成TapTable
        return tapResult.getData();
    }

    protected TapTable modelDeductionForTapTable(ConnectorNode connectorNode) {
        LinkedHashMap<String, TapField> modelDeduction = this.modelDeduction(connectorNode);
        TapTable tabled = new TapTable();
        modelDeduction.forEach((name, field) -> tabled.add(field));
        tabled.setName(this.targetTable.getName());
        tabled.setId(this.targetTable.getId());
        return tabled;
    }

    protected TapCreateTableEvent modelDeductionForCreateTableEvent(ConnectorNode connectorNode) {
        TapCreateTableEvent createTableEvent = new TapCreateTableEvent();
        createTableEvent.table(this.modelDeductionForTapTable(connectorNode));
        createTableEvent.setReferenceTime(System.currentTimeMillis());
        createTableEvent.setTableId(this.targetTable.getId());
        return createTableEvent;
    }

    public TapTable getTargetTable() {
        return this.targetTable;
    }
}
