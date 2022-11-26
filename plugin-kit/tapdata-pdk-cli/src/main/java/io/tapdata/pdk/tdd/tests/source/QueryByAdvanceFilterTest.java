package io.tapdata.pdk.tdd.tests.source;

import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
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

import static io.tapdata.entity.simplify.TapSimplify.map;


@DisplayName("Tests for source beginner test")
public class QueryByAdvanceFilterTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testTargetNodeId = "tt1";
    String testSourceNodeId = "ts1";
    String originNodeId = "r0";

    DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
    String originToSourceId;

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
            originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();

            TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
            dataFlowDescriber = new DAGDescriber();
            dataFlowDescriber.setId(originToSourceId);
            testTableId = testTableName(dataFlowDescriber.getId());

            dataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(originNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap()),
                    new TapDAGNodeEx().id(testTargetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(TapDAGNode.TYPE_TARGET/*nodeInfo.getNodeType()*/).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions)
            ));
            dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(originNodeId, testTargetNodeId)));
            dataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)).enableStreamRead(false));

            dag = dataFlowDescriber.toDag();
            if(dag != null) {
                dataFlowEngine.startDataFlow(dag, dataFlowDescriber.getJobOptions(), (fromState, toState, dataFlowWorker) -> {
                    if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
                        dataFlowEngine.sendExternalTapEvent(originToSourceId, new PatrolEvent().patrolListener((nodeId, state) -> {
                            TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
                            if (nodeId.equals(testTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
                                for (int i = 0; i < 10; i++) {
                                    DataMap dataMap = buildInsertRecord();
                                    dataMap.put("id", "id_" + i);
                                    sendInsertRecordEvent(dataFlowEngine, dag, tddTableId, dataMap);
                                    lastRecordToEqual = dataMap;
                                }
                                sendPatrolEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
                                    if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
//                                        startBatchReadTest();
                                        startQueryByAdvanceFilterTest(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode());
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

    private void startQueryByAdvanceFilterTest(ConnectorNode targetNode) throws Throwable {
        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();

        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(), TapAdvanceFilter.create().limit(2), targetTable, filterResults -> {
            $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
            $(() -> Assertions.assertEquals(2, filterResults.getResults().size(), "Limit 2 results"));
        });
        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", "12312323213")), targetTable, filterResults -> {
                    $(() -> Assertions.assertNull(filterResults.getResults(), "Query results should be null"));
                });

        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", "full_1")), targetTable, filterResults -> {
            $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
            $(() -> Assertions.assertEquals(1, filterResults.getResults().size(), "Should return 1 result"));
            $(() -> Assertions.assertEquals("123", filterResults.getResults().get(0).get("tap_string"), "tapString field should equal 123"));
              $(() -> Assertions.assertTrue(objectIsEqual(123.0, filterResults.getResults().get(0).get("tap_number")), "tapNumber field should equal 123.0"));
        });

        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lt("tap_int", 1023123)), targetTable, filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertEquals(10, filterResults.getResults().size(), "Should return 10 result"));
                    $(() -> Assertions.assertTrue(objectIsEqual("1234", filterResults.getResults().get(0).get("tap_string")), "tapString field should equal 1234"));
                    $(() -> Assertions.assertTrue(objectIsEqual(123.0, filterResults.getResults().get(0).get("tap_number")), "tapNumber field should equal 123.0"));
                    $(() -> Assertions.assertTrue(objectIsEqual(123123, filterResults.getResults().get(0).get("tap_int")), "tapInt field should equal 123123"));
                });
        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lte("tap_int", 1023123)), targetTable, filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertEquals(11, filterResults.getResults().size(), "Should return 11 result"));

                    $(() -> Assertions.assertTrue(objectIsEqual(1023123, filterResults.getResults().get(0).get("tap_int")), "First record, tapInt field should equal 1023123"));
                    $(() -> Assertions.assertTrue(objectIsEqual(123123, filterResults.getResults().get(1).get("tap_int")), "Second record, tapInt field should equal 123123"));
                });

        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lte("tap_int", 1023123)).skip(1).limit(1), targetTable, filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertEquals(1, filterResults.getResults().size(), "Should return 1 result"));

                    $(() -> Assertions.assertTrue(objectIsEqual(123123, filterResults.getResults().get(0).get("tap_int")), "First record, tapInt field should equal 123123"));
                });

        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lte("tap_int", 1023123)).skip(1).limit(1).projection(Projection.create().include("tap_int")), targetTable, filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertEquals(1, filterResults.getResults().size(), "Should return 1 result"));

                    $(() -> Assertions.assertTrue(objectIsEqual(123123, filterResults.getResults().get(0).get("tap_int")), "First record, tapInt field should equal 123123"));
//                    $(() -> Assertions.assertNull(filterResults.getResults().get(0).get("tapString"), "tapString should be removed by projection"));
                });

        sendDropTableEvent(dataFlowEngine, dag, testTableId, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
            if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {
                prepareConnectionNode(tapNodeInfo, connectionOptions, connectionNode -> {
                    PDKInvocationMonitor pdkInvocationMonitor = PDKInvocationMonitor.getInstance();
                    pdkInvocationMonitor.invokePDKMethod(connectionNode, PDKMethod.INIT, connectionNode::connectorInit, "Init", TAG);
                    String targetTable1 = dag.getNodeMap().get(testTargetNodeId).getTable();
                    List<TapTable> allTables = new ArrayList<>();
                    try {
                        connectionNode.discoverSchema(Collections.singletonList(targetTable1), 10, tables -> allTables.addAll(tables));
                        for(TapTable table : allTables) {
                            if(table.getName() != null && table.getId().equals(targetTable1)) {
                                $(() -> Assertions.fail("Target table " + targetTable1 + " should be deleted, because dropTable has been called, please check your dropTable method whether it works as expected or not"));
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
//        connectorFunctions.getQueryByAdvanceFilterFunction().query(targetNode.getConnectorContext(),
//                TapAdvanceFilter.create().op(), targetTable, filterResults -> {
//                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
//                    $(() -> Assertions.assertEquals(1, filterResults.getResults().size(), "Should return 1 result"));
//                    $(() -> Assertions.assertEquals("123", filterResults.getResults().get(0).get("tapString"), "tapString field should equal 123"));
//                    $(() -> Assertions.assertEquals(123.0, filterResults.getResults().get(0).get("tapNumber"), "tapNumber field should equal 123.0"));
//                });
//        queryByAdvanceFilterFunction.query(targetNode.getConnectorContext(), );
    }
/*

    private void startBatchReadTest() {
        if (dag != null) {
            JobOptions jobOptions = dataFlowDescriber.getJobOptions();
            dataFlowWorker = dataFlowEngine.startDataFlow(dag, jobOptions, (fromState, toState, dataFlowWorker) -> {
                if (toState.equals(DataFlowWorker.STATE_INITIALIZING)) {
                    initConnectorFunctions();
                    checkFunctions(sourceNode.getConnectorFunctions(), QueryByAdvanceFilterTest.testFunctions());
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
*/


    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types."),
                support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
    }

    protected void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public void tearDown() {
        super.tearDown();
    }
}
