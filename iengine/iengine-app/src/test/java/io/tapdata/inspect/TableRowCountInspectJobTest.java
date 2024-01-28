package io.tapdata.inspect;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.util.MetaType;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapCodeException;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

 class TableRowCountInspectJobTest {

     static InspectTask inspectTask;
     static ClientMongoOperator clientMongoOperator;

     static ConnectorNode sourceConnectorNode;

     static Connections source;
     static Connections target;

     static TapNodeSpecification tapNodeSpecification;

     static  ConnectorFunctions connectorFunction;

     static ConnectorNode targetConnectorNode;

     @BeforeAll
     static void init() {
         inspectTask = new InspectTask();
         inspectTask.setTaskId("test");
         clientMongoOperator = Mockito.mock(ClientMongoOperator.class);
         sourceConnectorNode = new ConnectorNode();
         source = new Connections();
         source.setName("testSource");
         target = new Connections();
         target.setName("targetSource");
         Map<String, Object> params = new HashMap<>();
         params.put("connectionId", null);
         params.put("metaType", MetaType.table.name());
         params.put("tableName", null);
         when(clientMongoOperator.findOne(params, ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/metadata/v2"
                 , TapTable.class)).thenReturn(Mockito.mock(TapTable.class));
         tapNodeSpecification = new TapNodeSpecification();
         tapNodeSpecification.setId("test");
         connectorFunction = new ConnectorFunctions();
         targetConnectorNode = new ConnectorNode();

     }

     @Test
     void testSourceCountByPartitionFilterFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         List<QueryOperator> list = new ArrayList<>();
         QueryOperator queryOperator = new QueryOperator();
         queryOperator.setKey("test");
         list.add(queryOperator);
         inspectSource.setConditions(list);
         inspectSource.setIsFilter(true);
         InspectDataSource inspectTarget = new InspectDataSource();
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);

         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);


         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, null, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);
         ReflectionTestUtils.setField(tableRowContentInspectJob, "progressUpdateCallback", Mockito.mock(ProgressUpdate.class));

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Source node does not support count with filter function"));


     }

     @Test
     void testTargetCountByPartitionFilterFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         InspectDataSource inspectTarget = new InspectDataSource();
         List<QueryOperator> list = new ArrayList<>();
         QueryOperator queryOperator = new QueryOperator();
         queryOperator.setKey("test");
         list.add(queryOperator);
         inspectTarget.setConditions(list);
         inspectTarget.setIsFilter(true);
         inspectSource.setConditions(list);
         inspectSource.setIsFilter(true);
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);



         connectorFunction.supportCountByPartitionFilterFunction(Mockito.mock(CountByPartitionFilterFunction.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);

         ReflectionTestUtils.setField(targetConnectorNode, "connectorFunctions", new ConnectorFunctions());
         ReflectionTestUtils.setField(targetConnectorNode, "connectorContext", tapConnectorContext);

         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, targetConnectorNode, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);
         ReflectionTestUtils.setField(tableRowContentInspectJob, "progressUpdateCallback", Mockito.mock(ProgressUpdate.class));

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Target node does not support count with filter function"));


     }


     @Test
     void testSourceExecuteCommandFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         inspectSource.setEnableCustomCommand(true);
         Map<String, Object> customCommand = new HashMap<>();
         customCommand.put("test", "value");
         inspectSource.setCustomCommand(customCommand);
         InspectDataSource inspectTarget = new InspectDataSource();
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);


         ConnectorFunctions connectorFunction = new ConnectorFunctions();
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);

         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, null, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);
         ReflectionTestUtils.setField(tableRowContentInspectJob, "progressUpdateCallback", Mockito.mock(ProgressUpdate.class));

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Source node does not support execute command function"));


     }

     @Test
     void testTargetExecuteCommandFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         inspectSource.setEnableCustomCommand(true);
         Map<String, Object> customCommand = new HashMap<>();
         customCommand.put("params", new HashMap<>());
         inspectSource.setCustomCommand(customCommand);
         InspectDataSource inspectTarget = new InspectDataSource();
         inspectTarget.setCustomCommand(customCommand);
         inspectTarget.setEnableCustomCommand(true);
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);


         connectorFunction.supportExecuteCommandFunction(Mockito.mock(ExecuteCommandFunction.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         TapNodeInfo tapNodeInfo = new TapNodeInfo();

         ReflectionTestUtils.setField(tapNodeInfo, "tapNodeSpecification", tapNodeSpecification);
         ReflectionTestUtils.setField(sourceConnectorNode, "tapNodeInfo", tapNodeInfo);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);

         ConnectorNode targetConnectorNode = new ConnectorNode();
         ReflectionTestUtils.setField(targetConnectorNode, "connectorFunctions", new ConnectorFunctions());
         ReflectionTestUtils.setField(targetConnectorNode, "connectorContext", tapConnectorContext);

         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, targetConnectorNode, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);
         ReflectionTestUtils.setField(tableRowContentInspectJob, "progressUpdateCallback", Mockito.mock(ProgressUpdate.class));

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Target node does not support execute command function"));


     }


     @Test
     void testSourceBatchFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         InspectDataSource inspectTarget = new InspectDataSource();
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);


         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);

         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, null, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);
         ReflectionTestUtils.setField(tableRowContentInspectJob, "progressUpdateCallback", Mockito.mock(ProgressUpdate.class));

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Source node does not support batch count function"));


     }

     @Test
     void testTargetBatchFunctionException() {
         // input param
         InspectDataSource inspectSource = new InspectDataSource();
         InspectDataSource inspectTarget = new InspectDataSource();
         inspectTask.setSource(inspectSource);
         inspectTask.setTarget(inspectTarget);


         connectorFunction.supportBatchCount(Mockito.mock(BatchCountFunction.class));

         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);

         TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorFunctions", connectorFunction);
         ReflectionTestUtils.setField(sourceConnectorNode, "connectorContext", tapConnectorContext);

         ReflectionTestUtils.setField(targetConnectorNode, "connectorFunctions", new ConnectorFunctions());
         ReflectionTestUtils.setField(targetConnectorNode, "connectorContext", tapConnectorContext);

         InspectTaskContext inspectTaskContext = new InspectTaskContext("test", inspectTask,
                 source, target, null, null, null, sourceConnectorNode, targetConnectorNode, clientMongoOperator);
         TableRowCountInspectJob tableRowContentInspectJob = new TableRowCountInspectJob(inspectTaskContext);

         ReflectionTestUtils.invokeMethod(tableRowContentInspectJob, "doRun");
         InspectResultStats inspectResultStats = (InspectResultStats) ReflectionTestUtils.getField(tableRowContentInspectJob, "stats");

         assert inspectResultStats != null;
         Assertions.assertTrue(inspectResultStats.getErrorMsg().contains("Target node does not support batch count function"));


     }

     @Test
     void testSetCommandCountParamException() {
         try {
             TableRowCountInspectJob.setCommandCountParam(new HashMap<>(), new ConnectorNode(), new TapTable());
         } catch (TapCodeException tapCodeException) {
             Assertions.assertEquals("27004", tapCodeException.getCode());
         }
     }

}
