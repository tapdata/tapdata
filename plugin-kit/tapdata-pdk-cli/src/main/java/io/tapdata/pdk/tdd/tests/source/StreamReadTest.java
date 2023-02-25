package io.tapdata.pdk.tdd.tests.source;

import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.core.workflow.engine.driver.SourceNodeDriver;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 1, use DAG tdd-source -> test-source write 1 record into test-source.
 * 2, use DAG test-source -> tdd-target only streamRead enabled.
 * 3, after DAG test-source -> tdd-target enter stream started state.
 * 4, use DAG tdd-source -> test-source write 10 records into test-source.
 * 5, test-source should be able to read them from streamRead method and write to tdd-target.
 * 6, wait 3 seconds, fetch all the records from tdd-target, verify 10 records has been received and match the last record with the record sent from test case.
 * 7, send dropTable event and verfy the table is actually dropped.
 *
 *
 */
@DisplayName("Tests for source beginner test")
public class StreamReadTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String tddTargetNodeId = "t2";
    String testSourceAsTargetNodeId = "tt1";
    String testSourceNodeId = "ts1";
    String tddSourceNodeId = "r0";

    DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
    String sourceToTddTargetDagId;
    String tddSourceToSourceAsTargetDagId;
    TapDAG tddToSourceDag;

    DAGDescriber dataFlowDescriber;

    DataMap lastRecordToEqual;
    int eventBatchSize = 5;

    TapNodeInfo tapNodeInfo;

    String tddTableId = "tdd-table";
    String testTableId;
    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            tapNodeInfo = nodeInfo;
            sourceToTddTargetDagId = "StreamReadTest_" + nodeInfo.getTapNodeSpecification().getId() + "ToTdd";
            tddSourceToSourceAsTargetDagId = "StreamReadTest_tddTo" + nodeInfo.getTapNodeSpecification().getId();

            TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
            // #1
            dataFlowDescriber = new DAGDescriber();
            dataFlowDescriber.setId(sourceToTddTargetDagId);
            testTableId = testTableName(dataFlowDescriber.getId());

            dataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(testSourceNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(/*nodeInfo.getNodeType()*/TapDAGNode.TYPE_SOURCE).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions),
                    new TapDAGNodeEx().id(tddTargetNodeId).pdkId("tdd-target").group("io.tapdata.connector").type(TapDAGNode.TYPE_TARGET).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap())
            ));
            dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(testSourceNodeId, tddTargetNodeId)));
            dataFlowDescriber.setJobOptions(new JobOptions()
                    .actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE))
                    .enableBatchRead(false)
                    .eventBatchSize(eventBatchSize));
            dag = dataFlowDescriber.toDag();

            // #2
            DAGDescriber originDataFlowDescriber = new DAGDescriber();
            originDataFlowDescriber.setId(tddSourceToSourceAsTargetDagId);

            originDataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(tddSourceNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap()),
                    new TapDAGNodeEx().id(testSourceAsTargetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(/*nodeInfo.getNodeType()*/TapDAGNode.TYPE_TARGET).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions)
            ));
            originDataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(tddSourceNodeId, testSourceAsTargetNodeId)));
            originDataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)));

            tddToSourceDag = originDataFlowDescriber.toDag();

            if(tddToSourceDag != null) {
                //Write initial records to source connector first. otherwise the source connector will be empty.
                dataFlowEngine.startDataFlow(tddToSourceDag, originDataFlowDescriber.getJobOptions(), (fromState, toState, dataFlowWorker) -> {
                    if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                        dataFlowEngine.sendExternalTapEvent(tddSourceToSourceAsTargetDagId, new PatrolEvent().patrolListener((nodeId, state) -> {
                            TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                            if (nodeId.equals(testSourceAsTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
//                                for (int i = 0; i < 10; i++) {
//                                    DataMap dataMap = buildInsertRecord();
//                                    dataMap.put("id", "id_" + i);
//                                    sendInsertRecordEvent(dataFlowEngine, tddToSourceDag, dataMap);
//                                    lastRecordToEqual = dataMap;
//                                }
                                sendPatrolEvent(dataFlowEngine, tddToSourceDag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                                    if (innerNodeId.equals(testSourceAsTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                                        //Start source to tddTarget connector
                                        sleep(3000L);
                                        startDag();
                                    }
                                }));

//                                    ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
//                                        PatrolEvent innerPatrolEvent = new PatrolEvent();
//                                        innerPatrolEvent.addInfo("callback", (Consumer<Integer>) streamCount -> {
//                                            Assertions.assertEquals(cnt.get(), streamCount);
//                                            completed();
//                                        });
//                                        dataFlowEngine.sendExternalTapEvent(sourceToTargetId, innerPatrolEvent);
//                                    }, 5, TimeUnit.SECONDS);

                            }
                        }));
                    }
                });
            }

        });
        waitCompleted(5000000);
    }

    private void startDag() {
        dataFlowEngine.startDataFlow(dag, dataFlowDescriber.getJobOptions(), (fromState, toState, dataFlowWorker) -> {
            if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                this.dataFlowWorker = dataFlowWorker;
                initConnectorFunctions();
                checkFunctions(sourceNode.getConnectorFunctions(), StreamReadTest.testFunctions());

                final long maxWaitSeconds = 60;
                long startTime = System.currentTimeMillis();
                final ScheduledFuture future = ExecutorsManager.getInstance().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                    if(System.currentTimeMillis() - startTime > TimeUnit.SECONDS.toMillis(maxWaitSeconds)) {
                        $(() -> fail("Failed to enter stream started state, which need use StreamReadConsumer#streamReadStarted in streamRead method to mark when stream is started. "));
                    } else {
                        TapLogger.info(TAG, "Waiting connector enter stream started state to do stream test, will timeout after {} second", maxWaitSeconds - ((System.currentTimeMillis() - startTime) / 1000));
                    }
                }, 5, 5, TimeUnit.SECONDS);
                dataFlowWorker.setSourceStateListener(state -> {
                    if(state == SourceNodeDriver.STATE_STREAM_STARTED) {
                        future.cancel(true);
                        startStreamReadTest();
                    }
                });
            }
        });
    }

    private void startStreamReadTest() {
        PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
            TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
            if (nodeId.equals(testSourceAsTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                AtomicInteger cnt = new AtomicInteger();
                sleep(5 * 1000L);
                for (int i = 0; i < 10; i++) {
                    DataMap dataMap = buildInsertRecord();
                    dataMap.put("id", "id_" + i);
                    lastRecordToEqual = dataMap;
                    sendInsertRecordEvent(dataFlowEngine, tddToSourceDag, tddTableId, dataMap, null);
                }
                sendPatrolEvent(dataFlowEngine, tddToSourceDag, new PatrolEvent().patrolListener((nodeId1, state1) -> {
                    if(nodeId1.equals(testSourceAsTargetNodeId) && state1 == PatrolEvent.STATE_LEAVE) {
                        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::verifyWriteTDDTargetResult, 3, TimeUnit.SECONDS);
                        TapLogger.info(TAG, "Wait stream read for 3 seconds, then verify result...");
                    }
                }));
//                        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> {
//                            PatrolEvent innerPatrolEvent = new PatrolEvent();
//                            innerPatrolEvent.addInfo("connectorCallback", (Consumer<Integer>) streamCount -> {
//                                Assertions.assertEquals(cnt.get(), streamCount);
//                                completed();
//                            });
//                            dataFlowEngine.sendExternalTapEvent(sourceToTargetId, innerPatrolEvent);
//                        }, 5, TimeUnit.SECONDS);

            }
        });
        dataFlowEngine.sendExternalTapEvent(tddSourceToSourceAsTargetDagId, patrolEvent);
    }

    private void verifyWriteTDDTargetResult() {
        PatrolEvent streamPatrolEvent = new PatrolEvent();
        streamPatrolEvent.addInfo("connectorCallback", (Consumer<Map<String, Object>>) stringObjectMap -> {
            Map<String, Map<String, Object>> primaryKeyRecordMap = (Map<String, Map<String, Object>>) stringObjectMap.get("primaryKeyRecordMap");
            List<List<TapRecordEvent>> batchList = (List<List<TapRecordEvent>>) stringObjectMap.get("batchList");

            assertNotNull(primaryKeyRecordMap, "Please check your streamRead method.");
            assertEquals(primaryKeyRecordMap.size(), 10, "10 records should be inserted, please check your streamRead method.");

            TapRecordEvent lastEvent = null;
            for(List<TapRecordEvent> batch : batchList) {
                if(!batch.isEmpty()) {
                    lastEvent = batch.get(batch.size() - 1);
                }
            }

            assertNotNull(lastEvent, "Last record in batchList should not be null");
            TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) lastEvent;
            StringBuilder builder = new StringBuilder();
            assertTrue(mapEquals(lastRecordToEqual, insertRecordEvent.getAfter(), builder), "Last record is not match " + builder.toString());

            //in originDag, mongodb connector is the target, the dropTableEvent can be handled as a target.
            sendDropTableEvent(dataFlowEngine, tddToSourceDag, testTableId, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                if (innerNodeId.equals(testSourceAsTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                    prepareConnectionNode(tapNodeInfo, connectionOptions, connectionNode -> {
                        PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
                        pdkInvocationMonitor.invokePDKMethod(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init", TAG);

                        String targetTable = tddToSourceDag.getNodeMap().get(testSourceAsTargetNodeId).getTable();

                        List<TapTable> allTables = new ArrayList<>();
                        try {
                            connectionNode.discoverSchema(Collections.singletonList(targetTable), 10, tables -> allTables.addAll(tables));
                            for(TapTable table : allTables) {
                                if(table.getId() != null && table.getId().equals(targetTable)) {
                                    $(() -> Assertions.fail("Target table " + targetTable + " should be deleted, because dropTable has been called, please check your dropTable method whether it works as expected or not"));
                                }
                            }

                            CommonUtils.handleAnyError(connectionNode::connectorStop);
                            completed();
                        } catch (Throwable throwable) {
                            throwable.printStackTrace();
                            Assertions.fail(throwable);
                        } finally {
                            CommonUtils.handleAnyError(connectionNode::connectorStop);
                        }
                    });
                }
            }));
        });
        dataFlowEngine.sendExternalTapEvent(sourceToTddTargetDagId, streamPatrolEvent);
    }

    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify streamRead, please implement it in registerCapabilities method."),
                support(StreamReadFunction.class, "StreamRead is a must to read incremental records, please implement it in registerCapabilities method."),
//                support(TimestampToStreamOffsetFunction.class, "StreamOffset is a must for incremental engine to record offset of stream read, please implement it in registerCapabilities method."),
                support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
    }

    protected void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(tddTargetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public void tearDown() {
        super.tearDown();
        if(tddToSourceDag != null) {
            DataFlowEngine.getInstance().stopDataFlow(tddToSourceDag.getId());
        }
    }
}
