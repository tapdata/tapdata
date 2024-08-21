package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableBeginAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableErrorAspect;
import io.tapdata.dao.DoSnapshotFunctions;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class HazelcastSourceConcurrentReadDataNodeTest {
    private HazelcastSourceConcurrentReadDataNode instance;
    private DataProcessorContext dataProcessorContext;
    private Processor.Context context;
    @BeforeEach
    void init(){
        context = mock(Processor.Context.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(context.hazelcastInstance()).thenReturn(hazelcastInstance);
        dataProcessorContext = mock(DataProcessorContext.class);
        TaskDto taskDto = mock(TaskDto.class);
        when(taskDto.getId()).thenReturn(mock(ObjectId.class));
        when(taskDto.getType()).thenReturn("initial_sync");
        when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
        Connections connections = mock(Connections.class);
        when(connections.getHeartbeatEnable()).thenReturn(false);
        when(dataProcessorContext.getConnections()).thenReturn(connections);
        instance = spy(new HazelcastSourceConcurrentReadDataNode(dataProcessorContext));
        doNothing().when(instance).createPdkConnectorNode(dataProcessorContext,hazelcastInstance);
        doNothing().when(instance).connectorNodeInit(dataProcessorContext);
        ObsLogger obsLogger = mock(ObsLogger.class);
        ReflectionTestUtils.setField(instance,"obsLogger",obsLogger);
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(instance,"clientMongoOperator",clientMongoOperator);
        ConnectorNode connectorNode = mock(ConnectorNode.class);
        when(instance.getConnectorNode()).thenReturn(connectorNode);
        TapConnectorContext tapContext = mock(TapConnectorContext.class);
        when(connectorNode.getConnectorContext()).thenReturn(tapContext);
        when(tapContext.getSpecification()).thenReturn(mock(TapNodeSpecification.class));
        ReflectionTestUtils.setField(instance,"endSnapshotLoop",mock(AtomicBoolean.class));
        SyncProgress syncProgress = mock(SyncProgress.class);
        when(syncProgress.getBatchOffsetObj()).thenReturn(new HashMap<>());
        ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
        TapTableMap<String, TapTable> tapTableMap = spy(TapTableMap.create("nodeId"));
        Map<String, String> tableNameAndQualifiedNameMap = new HashMap<>();
        tableNameAndQualifiedNameMap.put("table1","table1_qualifiedName");
        ReflectionTestUtils.setField(tapTableMap,"tableNameAndQualifiedNameMap",tableNameAndQualifiedNameMap);
        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getId()).thenReturn("table1");
        tapTableMap.put("table1", tapTable);
        when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
        doReturn(tapTable).when(tapTableMap).get("table1");
    }
    @Nested
    class doInitTest{
        @Test
        void notInstanceOfDatabaseNode(){
            Node node = mock(TableNode.class);
            when(dataProcessorContext.getNode()).thenReturn(node);
            assertThrows(RuntimeException.class, ()->instance.doInit(context));
        }
        @Test
        void testDoInitNormal(){
            Node node = mock(DatabaseNode.class);
            when(node.getId()).thenReturn("nodeId");
            when(node.getTaskId()).thenReturn("66a0da195b5c7e5d7b7e1c56");
            when(((DataParentNode<?>)node).getReadBatchSize()).thenReturn(100);
            when(((DatabaseNode)node).getConcurrentReadThreadNumber()).thenReturn(4);
            when(dataProcessorContext.getNode()).thenReturn(node);
            when(instance.getNode()).thenReturn(node);
            instance.doInit(context);
            assertEquals(4, instance.concurrentReadThreadNumber);
            assertNotNull(instance.concurrentReadThreadPool);
        }
    }

    @Test
    @SneakyThrows
    void doSnapshotTest(){
        List<String> tableList = new ArrayList<>();
        tableList.add("table1");
        int concurrentReadThreadNumber = 2;
        ReflectionTestUtils.setField(instance,"concurrentReadThreadNumber",concurrentReadThreadNumber);
        ConnectorNode connectorNode = mock(ConnectorNode.class);
        BatchCountFunction batchCountFunction = mock(BatchCountFunction.class);
        BatchReadFunction batchReadFunction = mock(BatchReadFunction.class);
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = mock(QueryByAdvanceFilterFunction.class);
        ExecuteCommandFunction executeCommandFunction = mock(ExecuteCommandFunction.class);
        DoSnapshotFunctions doSnapshotFunctions = new DoSnapshotFunctions(connectorNode, batchCountFunction, batchReadFunction, queryByAdvanceFilterFunction, executeCommandFunction);
        doReturn(doSnapshotFunctions).when(instance).checkFunctions(tableList);
        ExecutorService concurrentReadThreadPool = new ThreadPoolExecutor(concurrentReadThreadNumber, concurrentReadThreadNumber, 30L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        ReflectionTestUtils.setField(instance,"concurrentReadThreadPool",concurrentReadThreadPool);
        doNothing().when(instance).doSnapshotInvoke(anyString(), any(DoSnapshotFunctions.class), any(TapTable.class),any(AtomicBoolean.class),anyString());
        instance.doSnapshot(tableList);
        verify(instance,new Times(1)).processDoSnapshot(anyString(), any(AtomicBoolean.class));
        verify(instance,new Times(1)).doSnapshotInvoke(anyString(), any(DoSnapshotFunctions.class), any(TapTable.class),any(AtomicBoolean.class),anyString());
    }
    @Test
    @SneakyThrows
    void processDoSnapshotSkipTable(){
        try (MockedStatic<BatchOffsetUtil> mb = Mockito
                .mockStatic(BatchOffsetUtil.class)) {
            mb.when(()->BatchOffsetUtil.batchIsOverOfTable(any(SyncProgress.class),anyString())).thenReturn(true);
            String tableName = "table1";
            AtomicBoolean firstBatch = new AtomicBoolean(true);
            instance.processDoSnapshot(tableName, firstBatch);
            verify(instance,new Times(0)).executeAspect(any(SnapshotReadTableBeginAspect.class));
        }
    }
    @Test
    @SneakyThrows
    void processDoSnapshotForRemoveTable(){
        String tableName = "table1";
        AtomicBoolean firstBatch = new AtomicBoolean(true);
        CopyOnWriteArrayList<String> removeTables = new CopyOnWriteArrayList<>();
        removeTables.add("table1");
        ReflectionTestUtils.setField(instance,"removeTables",removeTables);
        instance.processDoSnapshot(tableName, firstBatch);
        verify(instance,new Times(0)).doSnapshotInvoke(anyString(), any(DoSnapshotFunctions.class), any(TapTable.class),any(AtomicBoolean.class),anyString());
    }
    @Test
    @SneakyThrows
    void processDoSnapshotWithEx(){
        String tableName = "table1";
        AtomicBoolean firstBatch = new AtomicBoolean(true);
        doThrow(RuntimeException.class).when(instance).doSnapshotInvoke(anyString(), any(DoSnapshotFunctions.class), any(TapTable.class),any(AtomicBoolean.class),anyString());
        assertThrows(TapCodeException.class,()->instance.processDoSnapshot(tableName, firstBatch));
        verify(instance,new Times(1)).executeAspect(any(SnapshotReadTableErrorAspect.class));
    }
    @Test
    void doCloseTest(){
        ExecutorService concurrentReadThreadPool = mock(ThreadPoolExecutor.class);
        ReflectionTestUtils.setField(instance,"concurrentReadThreadPool",concurrentReadThreadPool);
        when(instance.getNode()).thenReturn(mock(Node.class));
        instance.doClose();
        verify(concurrentReadThreadPool,new Times(1)).shutdown();
    }
}
