package io.tapdata.pdk.tdd.tests.source;

import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 1, use DAG tdd-source -> test-source write 11 record into test-source.
 * 2, after 11 records write to test-source.
 * 3, start DAG test-source -> tdd-target with batchRead and streamRead enabled.
 * 4, test-source should be able to read them from batchRead method and write to tdd-target.
 * 5, after 11 records write to tdd-target, fetch all the records from tdd-target, verify 11 records has been received and match the last record with the record sent from test case.
 * 6, send dropTable event and verify the table is actually dropped.
 *
 */
@DisplayName("Tests for source beginner test")
public class BatchReadTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testTargetNodeId = "tt1";
    String testSourceNodeId = "ts1";
    String originNodeId = "r0";

    DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
    String sourceToTargetId;
    String originToSourceId;
    TapDAG originDag;

    DAGDescriber dataFlowDescriber;

    DataMap lastRecordToEqual;
    int eventBatchSize = 5;

    TapNodeInfo tapNodeInfo;

    String testTableId;
    String tddTableId = "tdd-table";
    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            tapNodeInfo = nodeInfo;
            sourceToTargetId = "BatchReadTest_" + nodeInfo.getTapNodeSpecification().getId() + "ToTddTarget";
            originToSourceId = "BatchReadTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();

            TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
            // #1
            dataFlowDescriber = new DAGDescriber();
            dataFlowDescriber.setId(sourceToTargetId);
            testTableId = testTableName(dataFlowDescriber.getId());

            dataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(testSourceNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(TapDAGNode.TYPE_SOURCE/*nodeInfo.getNodeType()*/).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions),
                    new TapDAGNodeEx().id(targetNodeId).pdkId("tdd-target").group("io.tapdata.connector").type(TapDAGNode.TYPE_TARGET).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap())
            ));
            dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(testSourceNodeId, targetNodeId)));
            dataFlowDescriber.setJobOptions(new JobOptions()
                    .actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE))
                    .eventBatchSize(eventBatchSize));
            dag = dataFlowDescriber.toDag();

            // #2
            DAGDescriber originDataFlowDescriber = new DAGDescriber();
            originDataFlowDescriber.setId(originToSourceId);

            originDataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(originNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap()),
                    new TapDAGNodeEx().id(testTargetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(TapDAGNode.TYPE_TARGET/*nodeInfo.getNodeType()*/).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions)
            ));
            originDataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(originNodeId, testTargetNodeId)));
            originDataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)));

            originDag = originDataFlowDescriber.toDag();
            if(originDag != null) {
                dataFlowEngine.startDataFlow(originDag, originDataFlowDescriber.getJobOptions(), (fromState, toState, dataFlowWorker) -> {
                    if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                        dataFlowEngine.sendExternalTapEvent(originToSourceId, new PatrolEvent().patrolListener((nodeId, state) -> {
                            TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                            if (nodeId.equals(testTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                for (int i = 0; i < 10; i++) {
                                    DataMap dataMap = buildInsertRecord();
                                    dataMap.put("id", "id_" + i);
                                    sendInsertRecordEvent(dataFlowEngine, originDag, tddTableId, dataMap);
                                    lastRecordToEqual = dataMap;
                                }
                                sendPatrolEvent(dataFlowEngine, originDag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                                    if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                                        startBatchReadTest();
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

    private void startBatchReadTest() {
        if (dag != null) {
            JobOptions jobOptions = dataFlowDescriber.getJobOptions();
            dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                    initConnectorFunctions();
                    checkFunctions(sourceNode.getConnectorFunctions(), BatchReadTest.testFunctions());
                } else if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                    PatrolEvent patrolEvent = new PatrolEvent().patrolListener((nodeId, state) -> {
                        TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                        if (nodeId.equals(targetNodeId) && state == PatrolEvent.STATE_LEAVE) {
//                            processStreamInsert();
                            PatrolEvent callbackPatrol = new PatrolEvent();
                            callbackPatrol.addInfo("connectorCallback", (Consumer<Map<String, Object>>) stringObjectMap -> {
                                Map<String, Map<String, Object>> primaryKeyRecordMap = (Map<String, Map<String, Object>>) stringObjectMap.get("primaryKeyRecordMap");
                                List<List<TapRecordEvent>> batchList = (List<List<TapRecordEvent>>) stringObjectMap.get("batchList");

                                assertNotNull(primaryKeyRecordMap, "Please check your batchRead method.");
                                assertEquals(primaryKeyRecordMap.size(), 11, "11 records should be inserted, please check your batchRead method.");

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
                                sendDropTableEvent(dataFlowEngine, originDag, testTableId, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                                    if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                                        prepareConnectionNode(tapNodeInfo, connectionOptions, connectionNode -> {
                                            PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
                                            pdkInvocationMonitor.invokePDKMethod(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init", TAG);
                                            String targetTable = originDag.getNodeMap().get(testTargetNodeId).getTable();
                                            List<TapTable> allTables = new ArrayList<>();
                                            try {
                                                connectionNode.discoverSchema(Collections.singletonList(targetTable), 10, tables -> allTables.addAll(tables));
                                                for(TapTable table : allTables) {
                                                    if(table.getName() != null && table.getId().equals(targetTable)) {
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
                            dataFlowEngine.sendExternalTapEvent(sourceToTargetId, callbackPatrol);
                        }
                    });
                    dataFlowEngine.sendExternalTapEvent(sourceToTargetId, patrolEvent);
                }
            });
        }
    }


    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(BatchReadFunction.class, "BatchReadFunction is a must to read initial records, please implement it in registerCapabilities method."),
                support(BatchCountFunction.class, "BatchCountFunction is a must for the total size of initial records, please implement it in registerCapabilities method."),
                support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types."),
                support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
    }
}
