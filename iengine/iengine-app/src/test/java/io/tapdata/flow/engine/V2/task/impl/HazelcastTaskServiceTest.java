package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.mongodb.MongoClientException;
import com.tapdata.constant.*;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.JetDag;
import com.tapdata.entity.Setting;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.process.script.py.PyProcessNode;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ProcessorNodeType;
import io.github.openlg.graphlib.Graph;
import io.tapdata.MockTaskUtil;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.common.utils.StopWatch;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.*;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewInstance;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewService;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.ProcessAfterMergeUtil;
import io.tapdata.flow.engine.util.TaskDtoUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.core.api.impl.JsonParserImpl;
import io.tapdata.pdk.core.executor.ThreadFactory;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import io.tapdata.threadgroup.CpuMemoryCollector;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


public class HazelcastTaskServiceTest {
    @Nested
    class GetTaskRetryConfigTest {
        private HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void setUp() {
            HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
        }

        @DisplayName("test get task retry config default")
        @Test
        void test1() {
            SettingService settingService = mock(SettingService.class);
            when(settingService.getLong(eq("retry_interval_second"), eq(60L))).thenReturn(60L);
            when(settingService.getLong(eq("max_retry_time_minute"), eq(60L))).thenReturn(60L);
            ReflectionTestUtils.setField(hazelcastTaskService, "settingService", settingService);
            TaskRetryConfig taskRetryConfig = hazelcastTaskService.getTaskRetryConfig(new TaskDto());
            assertEquals(60L, taskRetryConfig.getRetryIntervalSecond());
            assertEquals(60L * 60, taskRetryConfig.getMaxRetryTimeSecond());
        }
    }

    @Nested
    class CreateNodeTest {
        private TaskDto taskDto;
        private List<Node> nodes;
        private List<Edge> edges;
        private Node node;
        private List<Node> predecessors;
        private List<Node> successors;
        private ConfigurationCenter config;
        private Connections connection;
        private DatabaseTypeEnum.DatabaseType databaseType;
        private Map<String, MergeTableNode> mergeTableMap;
        private TapTableMap<String, TapTable> tapTableMap;
        private TaskConfig taskConfig;

        @BeforeEach
        void beforeEach() {
            taskDto = mock(TaskDto.class);
            nodes = new ArrayList<>();
            edges = new ArrayList<>();
            node = mock(TableNode.class);
            predecessors = new ArrayList<>();
            successors = new ArrayList<>();
            config = mock(ConfigurationCenter.class);
            config = new ConfigurationCenter();
            config.putConfig(ConfigurationCenter.RETRY_TIME, 1);
            connection = mock(Connections.class);
            databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
            mergeTableMap = new HashMap<>();
            tapTableMap = mock(TapTableMap.class);
            taskConfig = mock(TaskConfig.class);
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method when read partition option is enable")
        void testCreateNode1() {
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode) node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePartitionReadDataNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method when read partition option is enable and task type is cdc")
        void testCreateNode2() {
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode) node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePdkDataNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast blank")
        void testCreateNode3() {
            node = mock(ProcessorNode.class);
            when(node.disabledNode()).thenReturn(true);
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastBlank.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast task source")
        void testCreateNode22() {
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode) node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTaskSource.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast target pdk data node")
        void testCreateNode23() {
            when(node.getType()).thenReturn("table");
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode) node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTargetPdkDataNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast task target")
        void testCreateNode24() {
            when(node.getType()).thenReturn("table");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode) node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTaskTarget.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast pdk source and target table node")
        void testCreateNode4() {
            when(node.getType()).thenReturn("table");
            predecessors.add(mock(Node.class));
            successors.add(mock(Node.class));
            when(connection.getPdkType()).thenReturn("pdk");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastPdkSourceAndTargetTableNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast task source and target")
        void testCreateNode25() {
            when(node.getType()).thenReturn("table");
            predecessors.add(mock(Node.class));
            successors.add(mock(Node.class));
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTaskSourceAndTarget.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for cache node")
        void testCreateNode5() {
            node = mock(CacheNode.class);
            try (MockedStatic<ExternalStorageUtil> mb = Mockito
                    .mockStatic(ExternalStorageUtil.class)) {
                mb.when(() -> ExternalStorageUtil.getExternalStorage(node)).thenReturn(mock(ExternalStorageDto.class));
                try (MockedStatic<HazelcastUtil> hazelcastUtilMockedStatic = Mockito
                        .mockStatic(HazelcastUtil.class)) {
                    hazelcastUtilMockedStatic.when(HazelcastUtil::getInstance).thenReturn(mock(HazelcastInstance.class));
                    ConnectorConstant.clientMongoOperator = mock(ClientMongoOperator.class);
                    ConfigurationCenter.processId = "11111";
                    when(node.getType()).thenReturn("mem_cache");
                    when(connection.getPdkType()).thenReturn("pdk");
                    when(taskDto.getType()).thenReturn("initial_sync");
                    HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
                    assertEquals(HazelcastTargetPdkCacheNode.class, actual.getClass());
                }
            }
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for cache target")
        void testCreateNode26() {
            node = mock(CacheNode.class);
            try (MockedStatic<ExternalStorageUtil> mb = Mockito
                    .mockStatic(ExternalStorageUtil.class)) {
                mb.when(() -> ExternalStorageUtil.getExternalStorage(node)).thenReturn(mock(ExternalStorageDto.class));
                try (MockedStatic<HazelcastUtil> hazelcastUtilMockedStatic = Mockito
                        .mockStatic(HazelcastUtil.class)) {
                    hazelcastUtilMockedStatic.when(HazelcastUtil::getInstance).thenReturn(mock(HazelcastInstance.class));
                    ConnectorConstant.clientMongoOperator = mock(ClientMongoOperator.class);
                    ConfigurationCenter.processId = "11111";
                    when(node.getType()).thenReturn("mem_cache");
                    when(taskDto.getType()).thenReturn("initial_sync");
                    HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
                    assertEquals(HazelcastCacheTarget.class, actual.getClass());
                }
            }
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for auto inspect node")
        void testCreateNode6() {
            node = mock(AutoInspectNode.class);
            when(((AutoInspectNode) node).getToNode()).thenReturn(mock(DatabaseNode.class));
            when(node.getType()).thenReturn("auto_inspect");
            when(connection.getPdkType()).thenReturn("pdk");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTargetPdkAutoInspectNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for virtual target node")
        void testCreateNode7() {
            when(node.getType()).thenReturn("VirtualTarget");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("testRun");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastVirtualTargetNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for join processor")
        void testCreateNode8() {
            HashSet set = new HashSet();
            set.add("test");
            when(tapTableMap.keySet()).thenReturn(set);
            when(node.getType()).thenReturn("join_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastJoinProcessor.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for js processor")
        void testCreateNode9() {
            when(node.getType()).thenReturn("js_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastJavaScriptProcessorNode.class, actual.getClass());
        }


        @Test
        @SneakyThrows
        @DisplayName("test createNode method for unwind processor")
        void testCreateNode11() {
            when(node.getType()).thenReturn("unwind_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastUnwindProcessNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for field processor")
        void testCreateNode12() {
            when(node.getType()).thenReturn("field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for table rename processor")
        void testCreateNode13() {
            node = mock(TableRenameProcessNode.class);
            when(node.getType()).thenReturn("table_rename_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastRenameTableProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate field rename processor")
        void testCreateNode14() {
            node = mock(MigrateFieldRenameProcessorNode.class);
            when(node.getType()).thenReturn("migrate_field_rename_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastMigrateFieldRenameProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for share cdc node")
        void testCreateNode15() {
            when(node.getType()).thenReturn("logCollector");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePdkShareCDCNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for custom processor")
        void testCreateNode17() {
            when(node.getType()).thenReturn("custom_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastCustomProcessor.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for merge table processor")
        void testCreateNode18() {
            when(node.getType()).thenReturn("merge_table_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastMergeNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for date processor")
        void testCreateNode19() {
            node = mock(DateProcessorNode.class);
            when(node.getType()).thenReturn("date_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            JsonParserImpl jsonParser = new JsonParserImpl();
            try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = mockStatic(InstanceFactory.class)) {
                instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(JsonParser.class)).thenReturn(jsonParser);
                HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
                assertEquals(HazelcastDateProcessorNode.class, actual.getClass());
            }
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for field mod type filter processor")
        void testCreateNode20() {
            when(node.getType()).thenReturn("field_mod_type_filter_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTypeFilterProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNode21() {
            node = mock(AddDateFieldProcessorNode.class);
            when(node.getType()).thenReturn("add_date_field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastAddDateFieldProcessNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNodeTestRunTask() {
            node = mock(AddDateFieldProcessorNode.class);
            when(node.getType()).thenReturn("add_date_field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("testRun");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastAddDateFieldProcessNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNodeDeduceSchemaTask() {
            node = mock(AddDateFieldProcessorNode.class);
            when(node.getType()).thenReturn("add_date_field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("deduceSchema");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastAddDateFieldProcessNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNodeSyncTask() {
            node = mock(AddDateFieldProcessorNode.class);
            when(node.getType()).thenReturn("add_date_field_processor");
            when(node.disabledNode()).thenReturn(true);
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastBlank.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate js processor")
        void testCreateNodeMigrateTask() {
            node = mock(MigrateJsProcessorNode.class);
            when(node.getType()).thenReturn("migrate_js_processor");
            when(node.disabledNode()).thenReturn(true);
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("migrate");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastBlank.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate js processor")
        void testCreateNodeMigrateTaskDisableIsFalse() {
            node = mock(MigrateJsProcessorNode.class);
            when(node.getType()).thenReturn("migrate_js_processor");
            when(node.disabledNode()).thenReturn(false);
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("migrate");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastJavaScriptProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate union processor")
        void testCreateNodeMigrateUnionProcessorNode() {
            node = mock(MigrateUnionProcessorNode.class);
            when(node.getType()).thenReturn("migrate_union_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("migrate");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastMigrateUnionProcessorNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate union processor")
        void testCreateNodeSourceConcurrentReadDataNode() {
            successors.add(mock(Node.class));
            node = mock(DatabaseNode.class);
            when(node.getType()).thenReturn("database");
            when(((DatabaseNode) node).isEnableConcurrentRead()).thenReturn(true);
            when(connection.getPdkType()).thenReturn("pdk");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("migrate");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourceConcurrentReadDataNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for huawei drs kafka convertor")
        void testCreateNodeHuaweiDrsKafkaConvertor() {
            node = mock(HuaweiDrsKafkaConvertorNode.class);
            when(node.getType()).thenReturn("huawei_drs_kafka_convertor");
            when(node.disabledNode()).thenReturn(false);
            when(connection.getPdkType()).thenReturn("pdk");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastHuaweiDrsKafkaConvertorNode.class, actual.getClass());
        }
    }

    @Nested
    class CleanAllUnselectedErrorTest {
        HazelcastTaskService hazelcastTaskService;
        TaskDto taskDto;
        ObsLogger obsLogger;
        ClientMongoOperator clientMongoOperator;

        List<ErrorEvent> errorEvents;
        ErrorEvent errorEvent;

        @BeforeEach
        void init() {
            clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = mock(HazelcastTaskService.class);
            ReflectionTestUtils.setField(hazelcastTaskService, "clientMongoOperator", clientMongoOperator);

            taskDto = mock(TaskDto.class);
            obsLogger = mock(ObsLogger.class);

            errorEvents = new ArrayList<>();
            errorEvent = new ErrorEvent();


            when(taskDto.getErrorEvents()).thenReturn(errorEvents);
            doNothing().when(taskDto).setErrorEvents(anyList());
            when(taskDto.getId()).thenReturn(new ObjectId());

            doCallRealMethod().when(hazelcastTaskService).cleanAllUnselectedError(taskDto, obsLogger);
        }

        @Test
        void testNormal() {
            errorEvents.add(errorEvent);
            try (MockedStatic<TaskDtoUtil> tdu = mockStatic(TaskDtoUtil.class)) {
                tdu.when(() -> TaskDtoUtil.updateErrorEvent(any(ClientMongoOperator.class), anyList(), any(ObjectId.class), any(ObsLogger.class), anyString())).thenAnswer(a -> null);
                hazelcastTaskService.cleanAllUnselectedError(taskDto, obsLogger);
                verify(taskDto).getErrorEvents();
                verify(taskDto).setErrorEvents(anyList());
                verify(taskDto).getId();
            }
        }

        @Test
        void testEmpty() {
            try (MockedStatic<TaskDtoUtil> tdu = mockStatic(TaskDtoUtil.class)) {
                tdu.when(() -> TaskDtoUtil.updateErrorEvent(any(ClientMongoOperator.class), anyList(), any(ObjectId.class), any(ObsLogger.class), anyString())).thenAnswer(a -> null);
                hazelcastTaskService.cleanAllUnselectedError(taskDto, obsLogger);
                verify(taskDto).getErrorEvents();
                verify(taskDto, times(0)).setErrorEvents(anyList());
                verify(taskDto, times(0)).getId();
            }
        }
    }

    @Nested
    class Task2HazelcastDAGTest {
        HazelcastTaskService hazelcastTaskService;
        ClientMongoOperator clientMongoOperator;
        ConfigurationCenter configurationCenter;

        @BeforeEach
        void init() {
            clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = mock(HazelcastTaskService.class);
            configurationCenter = mock(ConfigurationCenter.class);
            ReflectionTestUtils.setField(hazelcastTaskService, "clientMongoOperator", clientMongoOperator);
            ReflectionTestUtils.setField(hazelcastTaskService, "configurationCenter", configurationCenter);
        }

        @DisplayName("test task2HazelcastDAG method when node is tableNode")
        @Test
        void test1() {
            try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = mockStatic(HazelcastTaskService.class);
                 MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class)) {

                TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
                doCallRealMethod().when(hazelcastTaskService).task2HazelcastDAG(taskDto, true, true);

                when(hazelcastTaskService.getTaskConfig(any())).thenReturn(mock(TaskConfig.class));
                Connections connections = new Connections();
                connections.setPdkHash("dummy");
                when(hazelcastTaskService.getConnection(anyString())).thenReturn(connections);
                DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), any())).thenReturn(databaseType);

                HashMap<String, String> stringStringHashMap = new HashMap<>();
                stringStringHashMap.put("testNodeId", "testQualifiedName");
                HashMap<String, String> tableNameAndQualifiedNameMap = new HashMap();
                tableNameAndQualifiedNameMap.put("testNodeId", "1234");
                TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("testNodeId", tableNameAndQualifiedNameMap);
                tapTableMap.put("testNodeId", new TapTable());

                when(hazelcastTaskService.getTapTableMap(any(), any(), any(), any())).thenReturn(tapTableMap);
                hazelcastTaskService.task2HazelcastDAG(taskDto, true, true);
                verify(hazelcastTaskService, times(2)).singleTaskFilterEventDataIfNeed(eq(connections), any(), any());
            }
        }

        @DisplayName("test task2HazelcastDAG method when node is tableNode")
        @Test
        void test2() {
            try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = mockStatic(HazelcastTaskService.class);
                 MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class)) {

                TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
                doCallRealMethod().when(hazelcastTaskService).task2HazelcastDAG(taskDto, false, false);

                when(hazelcastTaskService.getTaskConfig(any())).thenReturn(mock(TaskConfig.class));
                Connections connections = new Connections();
                connections.setPdkHash("dummy");
                when(hazelcastTaskService.getConnection(anyString())).thenReturn(connections);
                DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), any())).thenReturn(databaseType);

                HashMap<String, String> stringStringHashMap = new HashMap<>();
                stringStringHashMap.put("testNodeId", "testQualifiedName");
                HashMap<String, String> tableNameAndQualifiedNameMap = new HashMap();
                tableNameAndQualifiedNameMap.put("testNodeId", "1234");
                TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("testNodeId", tableNameAndQualifiedNameMap);
                tapTableMap.put("testNodeId", new TapTable());

                when(hazelcastTaskService.getTapTableMap(any(), any(), any(), any())).thenReturn(tapTableMap);
                hazelcastTaskService.task2HazelcastDAG(taskDto, false, false);
                verify(hazelcastTaskService, times(2)).singleTaskFilterEventDataIfNeed(eq(connections), any(), any());
            }
        }
    }

    @Nested
    class SingleTaskFilterEventDataIfNeedTest {
        HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void init() {
            hazelcastTaskService = mock(HazelcastTaskService.class);
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is null")
        @Test
        void test1() {
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            TableNode tableNode = new TableNode();
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(null, needFilterEvent, tableNode);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(null, needFilterEvent, tableNode);
            assertEquals(true, needFilterEvent.get());
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when needFilterEvent is not null")
        @Test
        void test2() {
            Connections connections = new Connections();
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections, null, null);
            assertDoesNotThrow(() -> hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, null, null));
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is schema-free")
        @Test
        void test3() {
            Connections connections = new Connections();
            List<String> definitionTags = new ArrayList<>();
            definitionTags.add("schema-free");
            connections.setDefinitionTags(definitionTags);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(false, needFilterEvent.get());
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is not schema-free")
        @Test
        void test4() {
            Connections connections = new Connections();
            List<String> definitionTags = new ArrayList<>();
            definitionTags.add("Database");
            definitionTags.add("ssl");
            definitionTags.add("doubleActive");
            connections.setDefinitionTags(definitionTags);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(true, needFilterEvent.get());
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is not schema-free and tableNode is enableCustomSql")
        @Test
        void test5() {
            Connections connections = new Connections();
            List<String> definitionTags = new ArrayList<>();
            definitionTags.add("Database");
            definitionTags.add("ssl");
            definitionTags.add("doubleActive");
            connections.setDefinitionTags(definitionTags);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            TableNode tableNode = new TableNode();
            tableNode.setEnableCustomCommand(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections, needFilterEvent, tableNode);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, tableNode);
            assertEquals(false, needFilterEvent.get());
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections tags is null")
        @Test
        void test6() {
            Connections connections = new Connections();
            connections.setDefinitionTags(null);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(true, needFilterEvent.get());
        }


    }

    @Nested
    class EngineTransformSchemaTest {
        HazelcastTaskService hazelcastTaskService;
        ClientMongoOperator clientMongoOperator;

        @BeforeEach
        void init() {
            clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
        }

        @DisplayName("test initializeModel main process")
        @Test
        @Disabled
        void test1() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setSyncType("sync");
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try (MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                 MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)) {
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                TransformerWsMessageDto transformerWsMessageDto = new TransformerWsMessageDto();
                transformerWsMessageDto.setMetadataInstancesDtoList(new ArrayList<>());
                transformerWsMessageDto.setUserId("test");
                DAG.Options options = new DAG.Options();
                options.setUuid("test");
                transformerWsMessageDto.setOptions(options);
                transformerWsMessageDto.setDefinitionDtoMap(new HashMap<>());
                transformerWsMessageDto.setDataSourceMap(new HashMap<>());
                transformerWsMessageDto.setTransformerDtoMap(new HashMap<>());
                transformerWsMessageDto.setTaskDto(taskDto);
                mockedStatic.when(() -> AspectUtils.executeAspect(any(), any())).thenReturn(null);
                when(clientMongoOperator.findOne(any(Query.class), any(), any())).thenReturn(transformerWsMessageDto);
                DAG cloneDag = mock(DAG.class);

                when(dag.clone()).thenReturn(cloneDag);
                when(cloneDag.transformSchema(any(), any(), any())).thenReturn(new HashMap<>());
                Map<String, TapTableMap<String, TapTable>> result = hazelcastTaskService.engineTransformSchema(taskDto);
                Assertions.assertNotNull(result);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

        }


        @DisplayName("test initializeModel Deduction error")
        @Test
        void test2() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setSyncType("migrate");
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try (MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                 MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)) {
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                TransformerWsMessageDto transformerWsMessageDto = new TransformerWsMessageDto();
                transformerWsMessageDto.setMetadataInstancesDtoList(new ArrayList<>());
                transformerWsMessageDto.setUserId("test");
                DAG.Options options = new DAG.Options();
                options.setUuid("test");
                transformerWsMessageDto.setOptions(options);
                transformerWsMessageDto.setDefinitionDtoMap(new HashMap<>());
                transformerWsMessageDto.setDataSourceMap(new HashMap<>());
                transformerWsMessageDto.setTransformerDtoMap(new HashMap<>());
                transformerWsMessageDto.setTaskDto(taskDto);
                mockedStatic.when(() -> AspectUtils.executeAspect(any(), any())).thenReturn(null);
                when(clientMongoOperator.findOne(any(Query.class), any(), any())).thenReturn(transformerWsMessageDto);
                DAG cloneDag = mock(DAG.class);
                when(dag.clone()).thenReturn(cloneDag);
                doThrow(new MongoClientException("")).when(cloneDag).transformSchema(any(), any(), any(), any());
                Assertions.assertThrows(TapCodeException.class, () -> {
                    hazelcastTaskService.engineTransformSchema(taskDto);
                });
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

        }

        @DisplayName("test initializeModel get TransformerWsMessageDto error")
        @Test
        void test3() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setSyncType("migrate");
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try (MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                 MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)) {
                mockedStatic.when(() -> AspectUtils.executeAspect(any(), any())).thenReturn(null);
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Assertions.assertThrows(TapCodeException.class, () -> {
                    hazelcastTaskService.engineTransformSchema(taskDto);
                });
            }

        }
    }

    @Nested
    class GetTapTableMapTest {
        @Test
        void testTaskTypeIsTestRun() {
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("testRun");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            try (MockedStatic<TapTableUtil> tapTableUtilMockedStatic = mockStatic(TapTableUtil.class)) {
                tapTableUtilMockedStatic.when(() -> TapTableUtil.getTapTableMapByNodeId(anyString(), any())).thenAnswer(invocationOnMock -> {
                    Assertions.assertEquals("databaseNode", invocationOnMock.getArgument(0));
                    return null;
                });
                HazelcastTaskService.getTapTableMap(taskDto, 1L, databaseNode, new HashMap<>());
            }
        }

        @Test
        void testNormalTask() {
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            try (MockedStatic<TapTableMap> tableMapMockedStatic = mockStatic(TapTableMap.class)) {
                tableMapMockedStatic.when(() -> TapTableMap.create(anyString())).thenAnswer(invocationOnMock -> {
                    Assertions.assertEquals("databaseNode", invocationOnMock.getArgument(0));
                    return null;
                });
                HazelcastTaskService.getTapTableMap(taskDto, 1L, databaseNode, new HashMap<>());
            }
        }

        @Test
        void testNormalTaskTapTableMapHashMapIsNotNull() {
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
            TapTableMap<String, TapTable> except = TapTableMap.create("databaseNode");
            tapTableMapHashMap.put("databaseNode", except);
            TapTableMap<String, TapTable> result = HazelcastTaskService.getTapTableMap(taskDto, 1L, databaseNode, tapTableMapHashMap);
            Assertions.assertEquals(except, result);
        }
    }

    @Nested
    class CleanMergeNodeTest {
        HazelcastTaskService hazelcastTaskService = mock(HazelcastTaskService.class);

        @Test
        void test_cleanMergeNode() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            taskDto.setAttrs(new HashMap<>());
            List<Node> nodes = new ArrayList<>();
            MergeTableNode node = new MergeTableNode();
            node.setAttrs(new HashMap<String, Object>() {{
                put("disabled", false);
            }});
            nodes.add(node);
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
                when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
                beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
                doCallRealMethod().when(hazelcastTaskService).cleanMergeNode(taskDto, "test");
                hazelcastTaskService.cleanMergeNode(taskDto, "test");
                verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
            }
        }

        @Test
        void test_ContainDisabledNode() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            List<Node> nodes = new ArrayList<>();
            MergeTableNode node = new MergeTableNode();
            TableNode tableNode = new TableNode();
            tableNode.setAttrs(new HashMap<String, Object>() {{
                put("disabled", true);
            }});
            nodes.add(node);
            nodes.add(tableNode);
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
                when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
                beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
                doCallRealMethod().when(hazelcastTaskService).cleanMergeNode(taskDto, "test");
                hazelcastTaskService.cleanMergeNode(taskDto, "test");
                verify(clientMongoOperator, times(0)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
            }
        }

        @Test
        void test_TaskTypeCDC() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setType(TaskDto.TYPE_CDC);
            List<Node> nodes = new ArrayList<>();
            MergeTableNode node = new MergeTableNode();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("disabled", false);
            node.setAttrs(attrs);
            nodes.add(node);
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
                when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
                beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
                doCallRealMethod().when(hazelcastTaskService).cleanMergeNode(taskDto, "test");
                hazelcastTaskService.cleanMergeNode(taskDto, "test");
                verify(clientMongoOperator, times(0)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
            }
        }

        @Test
        void test_ContainSyncProgresse() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            Map<String, Object> taskAttrs = new HashMap<>();
            taskAttrs.put("syncProgress", new SyncProgress());
            taskDto.setAttrs(taskAttrs);
            List<Node> nodes = new ArrayList<>();
            MergeTableNode node = new MergeTableNode();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("disabled", false);
            node.setAttrs(attrs);
            nodes.add(node);
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
                when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
                beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
                doCallRealMethod().when(hazelcastTaskService).cleanMergeNode(taskDto, "test");
                hazelcastTaskService.cleanMergeNode(taskDto, "test");
                verify(clientMongoOperator, times(0)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
            }
        }

        @Test
        void test_NotNormalTask() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            taskDto.setPreview(true);
            List<Node> nodes = new ArrayList<>();
            MergeTableNode node = new MergeTableNode();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("disabled", false);
            node.setAttrs(attrs);
            nodes.add(node);
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
                when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
                beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
                doCallRealMethod().when(hazelcastTaskService).cleanMergeNode(taskDto, "test");
                hazelcastTaskService.cleanMergeNode(taskDto, "test");
                verify(clientMongoOperator, times(0)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
            }
        }
    }

    @Nested
    @DisplayName("Method startPreviewTask test")
    class startPreviewTaskTest {

        private HazelcastTaskService hazelcastTaskService;
        private HazelcastInstance hazelcastInstance;

        @BeforeEach
        void setUp() {
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
            hazelcastInstance = mock(HazelcastInstance.class);
            ReflectionTestUtils.setField(hazelcastTaskService, "hazelcastInstance", hazelcastInstance);
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
            taskDto.setPreview(true);
            JetDag jetDag = mock(JetDag.class);
            com.hazelcast.jet.core.DAG dag = mock(com.hazelcast.jet.core.DAG.class);
            when(jetDag.getDag()).thenReturn(dag);
            doReturn(jetDag).when(hazelcastTaskService).task2HazelcastDAG(taskDto, true, true);
            JetService jet = mock(JetService.class);
            when(hazelcastInstance.getJet()).thenReturn(jet);
            Job job = mock(Job.class);
            when(jet.newJob(eq(dag), any(JobConfig.class))).thenReturn(job);
            TaskClient<TaskDto> taskDtoTaskClient = assertDoesNotThrow(() ->  {
                try (MockedStatic<CpuMemoryCollector> cm = mockStatic(CpuMemoryCollector.class)) {
                    cm.when(() -> CpuMemoryCollector.startTask(taskDto)).thenAnswer(a -> null);
                    cm.when(() -> CpuMemoryCollector.addNode(anyString(), anyString())).thenAnswer(a -> null);
                    cm.when(() -> CpuMemoryCollector.registerTask(anyString(), any(ThreadFactory.class))).thenAnswer(a -> null);
                    cm.when(() -> CpuMemoryCollector.unregisterTask(anyString())).thenAnswer(a -> null);
                    cm.when(() -> CpuMemoryCollector.listening(anyString(), any(Object.class))).thenAnswer(a -> null);
                    cm.when(() -> CpuMemoryCollector.collectOnce(anyList())).thenReturn(new HashMap<>());
                    return hazelcastTaskService.startPreviewTask(taskDto);
                }
            });
            assertNotNull(taskDtoTaskClient);
            assertSame(job, ReflectionTestUtils.getField(taskDtoTaskClient, "job"));
            assertSame(taskDto, taskDtoTaskClient.getTask());
        }

        @Test
        @DisplayName("test sync type is testRun or deduceSchema")
        void test2() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
            taskDto.setPreview(true);
            JetDag jetDag = mock(JetDag.class);
            com.hazelcast.jet.core.DAG dag = mock(com.hazelcast.jet.core.DAG.class);
            when(jetDag.getDag()).thenReturn(dag);
            doReturn(jetDag).when(hazelcastTaskService).task2HazelcastDAG(eq(taskDto), any(Boolean.class), any(Boolean.class));
            JetService jet = mock(JetService.class);
            when(hazelcastInstance.getJet()).thenReturn(jet);
            Job job = mock(Job.class);
            when(jet.newLightJob(eq(dag), any(JobConfig.class))).thenReturn(job);

            taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
            assertDoesNotThrow(() -> hazelcastTaskService.startPreviewTask(taskDto));
            verify(hazelcastTaskService, times(1)).task2HazelcastDAG(taskDto, false, false);
            taskDto.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
            assertDoesNotThrow(() -> hazelcastTaskService.startPreviewTask(taskDto));
            verify(hazelcastTaskService, times(2)).task2HazelcastDAG(taskDto, false, false);
        }
    }

    @Nested
    @DisplayName("Method transformSchemaWhenPreview test")
    class transformSchemaWhenPreviewTest {

        private HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void setUp() {
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
            taskDto.setPreview(true);
            Node node = taskDto.getDag().getNodes().get(0);
            TapTableMap<String, TapTable> tapTableMap = mock(TapTableMap.class);
            Map<String, TapTableMap<String, TapTable>> nodeTapTableMap = new HashMap<>();
            nodeTapTableMap.put(node.getId(), tapTableMap);
            try (
                    MockedStatic<TaskPreviewService> taskPreviewServiceMockedStatic = mockStatic(TaskPreviewService.class)
            ) {
                TaskPreviewInstance taskPreviewInstance = mock(TaskPreviewInstance.class);
                when(taskPreviewInstance.getTapTableMapHashMap()).thenReturn(nodeTapTableMap);
                taskPreviewServiceMockedStatic.when(() -> TaskPreviewService.taskPreviewInstance(taskDto)).thenReturn(taskPreviewInstance);
                Map<String, TapTableMap<String, TapTable>> result = hazelcastTaskService.transformSchemaWhenPreview(taskDto);
                assertSame(result, nodeTapTableMap);
                verify(hazelcastTaskService, never()).engineTransformSchema(any(TaskDto.class));
            }
        }

        @Test
        @DisplayName("test previewQualifiedName is empty")
        void test2() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
            taskDto.setPreview(true);
            Node node = taskDto.getDag().getNodes().get(0);
            TableNode tableNode = assertInstanceOf(TableNode.class, node);
            TapTable tapTable = new TapTable("POLICY");
            tapTable.add(new TapField("POLICY_ID", "varchar(50)").tapType(new TapString()))
                    .add(new TapField("QUOTE_DAY", "datetime").tapType(new TapDateTime()));
            tableNode.setPreviewTapTable(tapTable);
            tableNode.setPreviewQualifiedName(null);
            Map mockNodeTapTableMap = mock(Map.class);
            doReturn(mockNodeTapTableMap).when(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
            Map<String, TapTableMap<String, TapTable>> nodeTapTableMap = hazelcastTaskService.transformSchemaWhenPreview(taskDto);
            assertSame(mockNodeTapTableMap, nodeTapTableMap);
            verify(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
        }

        @Test
        @DisplayName("test previewTapTable is empty")
        void test3() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
            taskDto.setPreview(true);
            Node node = taskDto.getDag().getNodes().get(0);
            TableNode tableNode = assertInstanceOf(TableNode.class, node);
            tableNode.setPreviewTapTable(null);
            tableNode.setPreviewQualifiedName("1");
            Map mockNodeTapTableMap = mock(Map.class);
            doReturn(mockNodeTapTableMap).when(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
            Map<String, TapTableMap<String, TapTable>> nodeTapTableMap = hazelcastTaskService.transformSchemaWhenPreview(taskDto);
            assertSame(mockNodeTapTableMap, nodeTapTableMap);
            verify(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
        }

        @Test
        @DisplayName("test is not table node")
        void test4() {
            TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview4.json");
            taskDto.setPreview(true);
            Map mockNodeTapTableMap = mock(Map.class);
            doReturn(mockNodeTapTableMap).when(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
            Map<String, TapTableMap<String, TapTable>> nodeTapTableMap = hazelcastTaskService.transformSchemaWhenPreview(taskDto);
            assertSame(mockNodeTapTableMap, nodeTapTableMap);
            verify(hazelcastTaskService).engineTransformSchema(any(TaskDto.class));
        }
    }

    @Nested
    @DisplayName("Method testTaskUsingPreview test")
    class testTaskUsingPreviewTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            Node<?> js = new JsProcessorNode();
            js.setId("js");
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setNode(js.getId(), js);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            graph.setEdge(merge.getId(), js.getId(), new Edge(merge.getId(), js.getId()));
            graph.setEdge(js.getId(), sink.getId(), new Edge(js.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            boolean result = hazelcastTaskService.testTaskUsingPreview(taskDto);
            assertTrue(result);
        }

        @Test
        @DisplayName("test standard js node")
        void test2() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            Node<?> js = new StandardJsProcessorNode();
            js.setId("js");
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setNode(js.getId(), js);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            graph.setEdge(merge.getId(), js.getId(), new Edge(merge.getId(), js.getId()));
            graph.setEdge(js.getId(), sink.getId(), new Edge(js.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            assertThrows(IllegalArgumentException.class, () -> hazelcastTaskService.testTaskUsingPreview(taskDto));
        }

        @Test
        @DisplayName("test js type=1")
        void test3() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            JsProcessorNode js = new JsProcessorNode();
            js.setId("js");
            js.setJsType(ProcessorNodeType.Standard_JS.type());
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setNode(js.getId(), js);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            graph.setEdge(merge.getId(), js.getId(), new Edge(merge.getId(), js.getId()));
            graph.setEdge(js.getId(), sink.getId(), new Edge(js.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            assertThrows(IllegalArgumentException.class, () -> hazelcastTaskService.testTaskUsingPreview(taskDto));
        }

        @Test
        @DisplayName("test python processor node")
        void test4() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            PyProcessNode python = new PyProcessNode();
            python.setId("python");
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setNode(python.getId(), python);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            graph.setEdge(merge.getId(), python.getId(), new Edge(merge.getId(), python.getId()));
            graph.setEdge(python.getId(), sink.getId(), new Edge(python.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            assertThrows(IllegalArgumentException.class, () -> hazelcastTaskService.testTaskUsingPreview(taskDto));
        }

        @Test
        void test5() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            FieldRenameProcessorNode fieldRename = new FieldRenameProcessorNode();
            fieldRename.setId("fieldRename");
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setNode(fieldRename.getId(), fieldRename);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            graph.setEdge(merge.getId(), fieldRename.getId(), new Edge(merge.getId(), fieldRename.getId()));
            graph.setEdge(fieldRename.getId(), sink.getId(), new Edge(fieldRename.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            boolean result = hazelcastTaskService.testTaskUsingPreview(taskDto);
            assertFalse(result);
        }

        @Test
        @DisplayName("test no merge node")
        void test6() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source = new TableNode();
            source.setId("source1");
            Node<?> sink = new TableNode();
            sink.setId("sink");
            graph.setNode(source.getId(), source);
            graph.setNode(sink.getId(), sink);
            graph.setEdge(source.getId(), sink.getId(), new Edge(source.getId(), sink.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            boolean result = hazelcastTaskService.testTaskUsingPreview(taskDto);
            assertFalse(result);
        }

        @Test
        @DisplayName("test merge node not have any successors")
        void test7() {
            Graph<Node, Edge> graph = new Graph<>();
            Node<?> source1 = new TableNode();
            source1.setId("source1");
            Node<?> source2 = new TableNode();
            source2.setId("source2");
            Node<?> merge = new MergeTableNode();
            merge.setId("merge");
            graph.setNode(source1.getId(), source1);
            graph.setNode(source2.getId(), source2);
            graph.setNode(merge.getId(), merge);
            graph.setEdge(source1.getId(), merge.getId(), new Edge(source1.getId(), merge.getId()));
            graph.setEdge(source2.getId(), merge.getId(), new Edge(source2.getId(), merge.getId()));
            TaskDto taskDto = new TaskDto();
            DAG dag = new DAG(graph);
            dag.getNodes().forEach(n -> n.setGraph(graph));
            taskDto.setDag(dag);

            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = new HazelcastTaskService(clientMongoOperator, clientMongoOperator);
            boolean result = hazelcastTaskService.testTaskUsingPreview(taskDto);
            assertFalse(result);
        }
    }

    @Nested
    @DisplayName("Method startTestTask test")
    class startTestTaskTest {
        @Test
        @DisplayName("run test task using preview")
        void test1() {
            TaskDto taskDto = new TaskDto();
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
            doReturn(true).when(hazelcastTaskService).testTaskUsingPreview(taskDto);
            try (
                    MockedStatic<InstanceFactory> instanceFactoryMockedStatic = mockStatic(InstanceFactory.class)
            ) {
                TaskPreviewService taskPreviewService = mock(TaskPreviewService.class);
                instanceFactoryMockedStatic.when(() -> InstanceFactory.bean(TaskPreviewService.class)).thenReturn(taskPreviewService);
                TaskClient<TaskDto> taskDtoTaskClient = hazelcastTaskService.startTestTask(taskDto);
                assertNull(taskDtoTaskClient);
                verify(taskPreviewService).previewTask(eq(taskDto), eq(Collections.EMPTY_LIST), eq(1), any(StopWatch.class));
            }
        }

        @Test
        @DisplayName("run test task not using preview")
        void test2() {
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
            taskDto.setDag(new DAG(new Graph<>()));
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            HazelcastTaskService hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
            doReturn(false).when(hazelcastTaskService).testTaskUsingPreview(taskDto);
            JetDag jetDag = mock(JetDag.class);
            doReturn(jetDag).when(hazelcastTaskService).task2HazelcastDAG(taskDto, false, false);
            com.hazelcast.jet.core.DAG dag = mock(com.hazelcast.jet.core.DAG.class);
            when(jetDag.getDag()).thenReturn(dag);
            HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
            ReflectionTestUtils.setField(hazelcastTaskService, "hazelcastInstance", hazelcastInstance);
            JetService jetService = mock(JetService.class);
            when(hazelcastInstance.getJet()).thenReturn(jetService);
            Job job = mock(Job.class);
            when(jetService.newLightJob(eq(dag), any(JobConfig.class))).thenReturn(job);

            try (
                    MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
            ) {
                ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(taskDto)).thenReturn(obsLogger);
                TaskClient<TaskDto> taskDtoTaskClient = assertDoesNotThrow(() -> hazelcastTaskService.startTestTask(taskDto));
                assertNotNull(taskDtoTaskClient);
                assertEquals(taskDto, taskDtoTaskClient.getTask());
                assertEquals(job, ReflectionTestUtils.getField(taskDtoTaskClient, "job"));
            }
        }
    }

    @Nested
    @DisplayName("Method handleDagWhenProcessAfterMerge test")
    class handleDagWhenProcessAfterMergeTest {

        private HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void setUp() {
            hazelcastTaskService = new HazelcastTaskService(null, null);
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            TaskDto taskDto = new TaskDto();
            try (
                    MockedStatic<ProcessAfterMergeUtil> processAfterMergeUtilMockedStatic = mockStatic(ProcessAfterMergeUtil.class);
                    MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
            ) {
                List<TableNode> tableNodes = new ArrayList<>();
                tableNodes.add(new TableNode() {{
                    setId("1");
                    setTableName("test1");
                    setConnectionId("1");
                }});
                tableNodes.add(new TableNode() {{
                    setId("2");
                    setTableName("test2");
                    setConnectionId("1");
                }});
                processAfterMergeUtilMockedStatic.when(() -> ProcessAfterMergeUtil.handleDagWhenProcessAfterMerge(taskDto)).thenReturn(tableNodes);
                ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(taskDto)).thenReturn(obsLogger);
                hazelcastTaskService.handleDagWhenProcessAfterMerge(taskDto);
                verify(obsLogger, times(3)).info(anyString());
            }
        }

        @Test
        @DisplayName("test no added nodes")
        void test2() {
            TaskDto taskDto = new TaskDto();
            try (
                    MockedStatic<ProcessAfterMergeUtil> processAfterMergeUtilMockedStatic = mockStatic(ProcessAfterMergeUtil.class);
                    MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
            ) {
                List<TableNode> tableNodes = new ArrayList<>();
                processAfterMergeUtilMockedStatic.when(() -> ProcessAfterMergeUtil.handleDagWhenProcessAfterMerge(taskDto)).thenReturn(tableNodes);
                ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(taskDto)).thenReturn(obsLogger);
                assertDoesNotThrow(() -> hazelcastTaskService.handleDagWhenProcessAfterMerge(taskDto));
                verify(obsLogger, never()).info(anyString());
            }
        }
    }

    @Nested
    @DisplayName("Method initSourceInitialCounter test")
    class initSourceInitialCounterTest {

        private HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void setUp() {
            hazelcastTaskService = new HazelcastTaskService(null, null);
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("67c7b94b027a1a13bd357f39"));
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            Node source1 = spy(new TableNode());
            doReturn(true).when(source1).disabledNode();
            List<Node> successors = new ArrayList<>();
            MergeTableNode successor = spy(new MergeTableNode());
            List<Node> mergeSuccessor = new ArrayList<>();

            successors.add(successor);
            doReturn(successors).when(source1).successors();
            Node source2 = spy(new TableNode());
            doReturn(successors).when(source2).successors();
            Node mergeNode = spy(new MergeTableNode());
            doReturn(successors).when(mergeNode).successors();
            Node target = spy(new TableNode());
            doReturn(new ArrayList<>()).when(target).successors();
            target.setId("67c7b94b027a1a13bd357f37");
            mergeSuccessor.add(target);
            doReturn(mergeSuccessor).when(successor).successors();
            nodes.add(source1);
            nodes.add(source2);
            nodes.add(mergeNode);
            nodes.add(target);
            when(dag.getNodes()).thenReturn(nodes);
            taskDto.setDag(dag);
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            hazelcastTaskService.initSourceInitialCounter(taskDto);
            Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(taskDto.getId().toHexString());
            String key = String.join("_", TaskGlobalVariable.SOURCE_INITIAL_COUNTER_KEY, target.getId());
            assertEquals(1, ((AtomicInteger)taskGlobalVariable.get(key)).get());

        }
    }

    @Nested
    @DisplayName("Method sourceAndSinkIsomorphismType test")
    class sourceAndSinkIsomorphismTypeTest {

		private HazelcastTaskService hazelcastTaskService;

        @BeforeEach
        void setUp() {
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            TaskDto taskDto = new TaskDto();
            String pdkId = "mongodb";
            DatabaseTypeEnum.DatabaseType srcDatabaseType = new DatabaseTypeEnum.DatabaseType();
            DatabaseTypeEnum.DatabaseType tgtDatabaseType = new DatabaseTypeEnum.DatabaseType();
            srcDatabaseType.setPdkId(pdkId);
            tgtDatabaseType.setPdkId(pdkId);
            Connections srcConn = new Connections();
            Connections tgtConn = new Connections();
            srcConn.setId("1");
            tgtConn.setId("2");
            srcConn.setPdkHash("1");
            tgtConn.setPdkHash("2");
            TableNode srcNode = new TableNode();
            TableNode tgtNode = new TableNode();
            srcNode.setId("1");
            tgtNode.setId("2");
            srcNode.setConnectionId(srcConn.getId());
            tgtNode.setConnectionId(tgtConn.getId());
            doReturn(srcConn).when(hazelcastTaskService).getConnection(srcNode.getConnectionId());
            doReturn(tgtConn).when(hazelcastTaskService).getConnection(tgtNode.getConnectionId());
            Graph<Node, Edge> graph = new Graph<>();
            graph.setNode(srcNode.getId(), srcNode);
            graph.setNode(tgtNode.getId(), tgtNode);
            graph.setEdge(srcNode.getId(), tgtNode.getId(), new Edge(srcNode.getId(), tgtNode.getId()));
            DAG dag = new DAG(graph);
            taskDto.setDag(dag);

            try (
                    MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class)
            ) {
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(ClientMongoOperator.class), eq(srcConn.getPdkHash()))).thenReturn(srcDatabaseType);
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(ClientMongoOperator.class), eq(tgtConn.getPdkHash()))).thenReturn(tgtDatabaseType);
                TapConnectorContext.IsomorphismType isomorphismType = hazelcastTaskService.sourceAndSinkIsomorphismType(taskDto);
                assertEquals(TapConnectorContext.IsomorphismType.ISOMORPHISM, isomorphismType);
            }
        }

        @Test
        @DisplayName("test different pdk id")
        void test2() {
            TaskDto taskDto = new TaskDto();
            String pdkId1 = "oracle";
            String pdkId2 = "mongodb";
            DatabaseTypeEnum.DatabaseType srcDatabaseType = new DatabaseTypeEnum.DatabaseType();
            DatabaseTypeEnum.DatabaseType tgtDatabaseType = new DatabaseTypeEnum.DatabaseType();
            srcDatabaseType.setPdkId(pdkId1);
            tgtDatabaseType.setPdkId(pdkId2);
            Connections srcConn = new Connections();
            Connections tgtConn = new Connections();
            srcConn.setId("1");
            tgtConn.setId("2");
            srcConn.setPdkHash("1");
            tgtConn.setPdkHash("2");
            TableNode srcNode = new TableNode();
            TableNode tgtNode = new TableNode();
            srcNode.setId("1");
            tgtNode.setId("2");
            srcNode.setConnectionId(srcConn.getId());
            tgtNode.setConnectionId(tgtConn.getId());
            doReturn(srcConn).when(hazelcastTaskService).getConnection(srcNode.getConnectionId());
            doReturn(tgtConn).when(hazelcastTaskService).getConnection(tgtNode.getConnectionId());
            Graph<Node, Edge> graph = new Graph<>();
            graph.setNode(srcNode.getId(), srcNode);
            graph.setNode(tgtNode.getId(), tgtNode);
            graph.setEdge(srcNode.getId(), tgtNode.getId(), new Edge(srcNode.getId(), tgtNode.getId()));
            DAG dag = new DAG(graph);
            taskDto.setDag(dag);

            try (
                    MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class)
            ) {
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(ClientMongoOperator.class), eq(srcConn.getPdkHash()))).thenReturn(srcDatabaseType);
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(ClientMongoOperator.class), eq(tgtConn.getPdkHash()))).thenReturn(tgtDatabaseType);
                TapConnectorContext.IsomorphismType isomorphismType = hazelcastTaskService.sourceAndSinkIsomorphismType(taskDto);
                assertEquals(TapConnectorContext.IsomorphismType.HETEROGENEOUS, isomorphismType);
            }
        }

        @Test
        @DisplayName("test empty graph")
        void test3() {
            TaskDto taskDto = new TaskDto();
            Graph<Node, Edge> graph = new Graph<>();
            DAG dag = new DAG(graph);
            taskDto.setDag(dag);
            TapConnectorContext.IsomorphismType isomorphismType = hazelcastTaskService.sourceAndSinkIsomorphismType(taskDto);
            assertEquals(TapConnectorContext.IsomorphismType.HETEROGENEOUS, isomorphismType);
        }

        @Test
        @DisplayName("test no data node")
        void test4() {
            TaskDto taskDto = new TaskDto();
            JsProcessorNode srcNode = new JsProcessorNode();
            srcNode.setId("1");
            Graph<Node, Edge> graph = new Graph<>();
            graph.setNode(srcNode.getId(), srcNode);
            DAG dag = new DAG(graph);
            taskDto.setDag(dag);
            TapConnectorContext.IsomorphismType isomorphismType = hazelcastTaskService.sourceAndSinkIsomorphismType(taskDto);
            assertEquals(TapConnectorContext.IsomorphismType.HETEROGENEOUS, isomorphismType);
        }
    }

    @Nested
    @DisplayName("Method openAutoIncrementalBatchSize test")
    class OpenAutoIncrementalBatchSizeTest {
        private HazelcastTaskService hazelcastTaskService;
        private SettingService settingService;

        @BeforeEach
        void setUp() {
            HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator, clientMongoOperator));
            settingService = mock(SettingService.class);
            ReflectionTestUtils.setField(hazelcastTaskService, "settingService", settingService);
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when setting is null")
        void testOpenAutoIncrementalBatchSizeWhenSettingNull() {
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(null);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertFalse(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is 'true'")
        void testOpenAutoIncrementalBatchSizeWhenValueIsTrue() {
            Setting setting = new Setting();
            setting.setValue("true");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertTrue(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is 'TRUE'")
        void testOpenAutoIncrementalBatchSizeWhenValueIsTRUE() {
            Setting setting = new Setting();
            setting.setValue("TRUE");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertTrue(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is 'false'")
        void testOpenAutoIncrementalBatchSizeWhenValueIsFalse() {
            Setting setting = new Setting();
            setting.setValue("false");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertFalse(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is null and default_value is 'true'")
        void testOpenAutoIncrementalBatchSizeWhenValueNullAndDefaultTrue() {
            Setting setting = new Setting();
            setting.setValue(null);
            setting.setDefault_value("true");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertTrue(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is null and default_value is 'TRUE'")
        void testOpenAutoIncrementalBatchSizeWhenValueNullAndDefaultTRUE() {
            Setting setting = new Setting();
            setting.setValue(null);
            setting.setDefault_value("TRUE");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertTrue(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is null and default_value is 'false'")
        void testOpenAutoIncrementalBatchSizeWhenValueNullAndDefaultFalse() {
            Setting setting = new Setting();
            setting.setValue(null);
            setting.setDefault_value("false");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertFalse(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when both value and default_value are null")
        void testOpenAutoIncrementalBatchSizeWhenBothNull() {
            Setting setting = new Setting();
            setting.setValue(null);
            setting.setDefault_value(null);
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertFalse(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }

        @Test
        @DisplayName("test openAutoIncrementalBatchSize when value is other string")
        void testOpenAutoIncrementalBatchSizeWhenValueIsOtherString() {
            Setting setting = new Setting();
            setting.setValue("enabled");
            when(settingService.getSetting("auto_incremental_batch_size")).thenReturn(setting);

            boolean result = hazelcastTaskService.openAutoIncrementalBatchSize();

            assertFalse(result);
            verify(settingService, times(1)).getSetting("auto_incremental_batch_size");
        }
    }
}
