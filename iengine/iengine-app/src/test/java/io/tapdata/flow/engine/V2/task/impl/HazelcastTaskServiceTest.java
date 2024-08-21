package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.MongoClientException;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.*;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.util.TaskDtoUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;


public class HazelcastTaskServiceTest {
    @Nested
    class GetTaskRetryConfigTest{
        private HazelcastTaskService hazelcastTaskService;
        @BeforeEach
        void setUp(){
            HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator));
        }
        @DisplayName("test get task retry config default")
        @Test
        void test1(){
            SettingService settingService = mock(SettingService.class);
            when(settingService.getLong(eq("retry_interval_second"), eq(60L))).thenReturn(60L);
            when(settingService.getLong(eq("max_retry_time_minute"), eq(60L))).thenReturn(60L);
            ReflectionTestUtils.setField(hazelcastTaskService, "settingService", settingService);
            TaskRetryConfig taskRetryConfig = hazelcastTaskService.getTaskRetryConfig();
            assertEquals(60L, taskRetryConfig.getRetryIntervalSecond());
            assertEquals(60L * 60, taskRetryConfig.getMaxRetryTimeSecond());
        }
    }
    @Nested
    class CreateNodeTest{
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
        void beforeEach(){
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
        void testCreateNode1(){
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode)node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePartitionReadDataNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method when read partition option is enable and task type is cdc")
        void testCreateNode2(){
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode)node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePdkDataNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast blank")
        void testCreateNode3(){
            node = mock(ProcessorNode.class);
            when(node.disabledNode()).thenReturn(true);
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastBlank.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast task source")
        void testCreateNode22(){
            when(node.getType()).thenReturn("table");
            successors.add(mock(Node.class));
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode)node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTaskSource.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast target pdk data node")
        void testCreateNode23(){
            when(node.getType()).thenReturn("table");
            when(connection.getPdkType()).thenReturn("pdk");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode)node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTargetPdkDataNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast task target")
        void testCreateNode24(){
            when(node.getType()).thenReturn("table");
            ReadPartitionOptions readPartitionOptions = mock(ReadPartitionOptions.class);
            when(((DataParentNode)node).getReadPartitionOptions()).thenReturn(readPartitionOptions);
            when(readPartitionOptions.isEnable()).thenReturn(true);
            when(readPartitionOptions.getSplitType()).thenReturn(10);
            when(taskDto.getType()).thenReturn("cdc");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTaskTarget.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for hazelcast pdk source and target table node")
        void testCreateNode4(){
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
        void testCreateNode25(){
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
        void testCreateNode5(){
            node = mock(CacheNode.class);
            try (MockedStatic<ExternalStorageUtil> mb = Mockito
                    .mockStatic(ExternalStorageUtil.class)) {
                mb.when(()->ExternalStorageUtil.getExternalStorage(node)).thenReturn(mock(ExternalStorageDto.class));
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
        void testCreateNode26(){
            node = mock(CacheNode.class);
            try (MockedStatic<ExternalStorageUtil> mb = Mockito
                    .mockStatic(ExternalStorageUtil.class)) {
                mb.when(()->ExternalStorageUtil.getExternalStorage(node)).thenReturn(mock(ExternalStorageDto.class));
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
        void testCreateNode6(){
            node = mock(AutoInspectNode.class);
            when(((AutoInspectNode)node).getToNode()).thenReturn(mock(DatabaseNode.class));
            when(node.getType()).thenReturn("auto_inspect");
            when(connection.getPdkType()).thenReturn("pdk");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTargetPdkAutoInspectNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for virtual target node")
        void testCreateNode7(){
            when(node.getType()).thenReturn("VirtualTarget");
            when(taskDto.getType()).thenReturn("initial_sync");
            when(taskDto.getSyncType()).thenReturn("testRun");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastVirtualTargetNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for join processor")
        void testCreateNode8(){
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
        void testCreateNode9(){
            when(node.getType()).thenReturn("js_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastJavaScriptProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for python processor")
        void testCreateNode10(){
            when(node.getType()).thenReturn("python_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastPythonProcessNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for unwind processor")
        void testCreateNode11(){
            when(node.getType()).thenReturn("unwind_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastUnwindProcessNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for field processor")
        void testCreateNode12(){
            when(node.getType()).thenReturn("field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for table rename processor")
        void testCreateNode13(){
            node = mock(TableRenameProcessNode.class);
            when(node.getType()).thenReturn("table_rename_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastRenameTableProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for migrate field rename processor")
        void testCreateNode14(){
            node = mock(MigrateFieldRenameProcessorNode.class);
            when(node.getType()).thenReturn("migrate_field_rename_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastMigrateFieldRenameProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for share cdc node")
        void testCreateNode15(){
            when(node.getType()).thenReturn("logCollector");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastSourcePdkShareCDCNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for custom processor")
        void testCreateNode17(){
            when(node.getType()).thenReturn("custom_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastCustomProcessor.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for merge table processor")
        void testCreateNode18(){
            when(node.getType()).thenReturn("merge_table_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastMergeNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for date processor")
        void testCreateNode19(){
            node = mock(DateProcessorNode.class);
            when(node.getType()).thenReturn("date_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastDateProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for field mod type filter processor")
        void testCreateNode20(){
            when(node.getType()).thenReturn("field_mod_type_filter_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTypeFilterProcessorNode.class, actual.getClass());
        }
        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNode21(){
            node = mock(AddDateFieldProcessorNode.class);
            when(node.getType()).thenReturn("add_date_field_processor");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastAddDateFieldProcessNode.class, actual.getClass());
        }

        @Test
        @SneakyThrows
        @DisplayName("test createNode method for add date field processor")
        void testCreateNodeTestRunTask(){
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
        void testCreateNodeDeduceSchemaTask(){
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
        void testCreateNodeSyncTask(){
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
        void testCreateNodeMigrateTask(){
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
        void testCreateNodeMigrateTaskDisableIsFalse(){
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
		void testCreateNodeMigrateUnionProcessorNode(){
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
        void testCreateNodeSourceConcurrentReadDataNode(){
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
            try(MockedStatic<TaskDtoUtil> tdu = mockStatic(TaskDtoUtil.class)) {
                tdu.when(() -> TaskDtoUtil.updateErrorEvent(any(ClientMongoOperator.class), anyList(), any(ObjectId.class), any(ObsLogger.class), anyString())).thenAnswer(a -> null);
                hazelcastTaskService.cleanAllUnselectedError(taskDto, obsLogger);
                verify(taskDto).getErrorEvents();
                verify(taskDto).setErrorEvents(anyList());
                verify(taskDto).getId();
            }
        }

        @Test
        void testEmpty() {
            try(MockedStatic<TaskDtoUtil> tdu = mockStatic(TaskDtoUtil.class)) {
                tdu.when(() -> TaskDtoUtil.updateErrorEvent(any(ClientMongoOperator.class), anyList(), any(ObjectId.class), any(ObsLogger.class), anyString())).thenAnswer(a -> null);
                hazelcastTaskService.cleanAllUnselectedError(taskDto, obsLogger);
                verify(taskDto).getErrorEvents();
                verify(taskDto, times(0)).setErrorEvents(anyList());
                verify(taskDto, times(0)).getId();
            }
        }
    }
    @Nested
    class Task2HazelcastDAGTest{
        HazelcastTaskService hazelcastTaskService;
        ClientMongoOperator clientMongoOperator;
        ConfigurationCenter configurationCenter;
        @BeforeEach
        void init(){
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
                doCallRealMethod().when(hazelcastTaskService).task2HazelcastDAG(taskDto,true);

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

                when(hazelcastTaskService.getTapTableMap(any(), any(), any(),any())).thenReturn(tapTableMap);
                hazelcastTaskService.task2HazelcastDAG(taskDto,true);
                verify(hazelcastTaskService, times(2)).singleTaskFilterEventDataIfNeed(eq(connections), any(), any());
            }
        }

        @DisplayName("test task2HazelcastDAG method when node is tableNode")
        @Test
        void test2() {
            try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = mockStatic(HazelcastTaskService.class);
                 MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class)) {

                TaskDto taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
                doCallRealMethod().when(hazelcastTaskService).task2HazelcastDAG(taskDto,false);

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

                when(hazelcastTaskService.getTapTableMap(any(), any(), any(),any())).thenReturn(tapTableMap);
                hazelcastTaskService.task2HazelcastDAG(taskDto,false);
                verify(hazelcastTaskService, times(2)).singleTaskFilterEventDataIfNeed(eq(connections), any(), any());
            }
        }
    }
    @Nested
    class SingleTaskFilterEventDataIfNeedTest{
        HazelcastTaskService hazelcastTaskService;
        @BeforeEach
        void init(){
            hazelcastTaskService = mock(HazelcastTaskService.class);
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is null")
        @Test
        void test1() {
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            TableNode tableNode = new TableNode();
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(null,needFilterEvent,tableNode);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(null, needFilterEvent, tableNode);
            assertEquals(true, needFilterEvent.get());
        }

        @DisplayName("test SingleTaskFilterEventDataIfNeed when needFilterEvent is not null")
        @Test
        void test2() {
            Connections connections = new Connections();
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections,null,null);
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
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections,needFilterEvent,null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(false, needFilterEvent.get());
        }
        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is not schema-free")
        @Test
        void test4(){
            Connections connections = new Connections();
            List<String> definitionTags = new ArrayList<>();
            definitionTags.add("Database");
            definitionTags.add("ssl");
            definitionTags.add("doubleActive");
            connections.setDefinitionTags(definitionTags);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections,needFilterEvent,null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(true, needFilterEvent.get());
        }
        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections is not schema-free and tableNode is enableCustomSql")
        @Test
        void test5(){
            Connections connections = new Connections();
            List<String> definitionTags = new ArrayList<>();
            definitionTags.add("Database");
            definitionTags.add("ssl");
            definitionTags.add("doubleActive");
            connections.setDefinitionTags(definitionTags);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            TableNode tableNode=new TableNode();
            tableNode.setEnableCustomCommand(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections,needFilterEvent,tableNode);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, tableNode);
            assertEquals(false, needFilterEvent.get());
        }
        @DisplayName("test SingleTaskFilterEventDataIfNeed when connections tags is null")
        @Test
        void test6(){
            Connections connections = new Connections();
            connections.setDefinitionTags(null);
            AtomicBoolean needFilterEvent = new AtomicBoolean(true);
            doCallRealMethod().when(hazelcastTaskService).singleTaskFilterEventDataIfNeed(connections,needFilterEvent,null);
            hazelcastTaskService.singleTaskFilterEventDataIfNeed(connections, needFilterEvent, null);
            assertEquals(true, needFilterEvent.get());
        }


    }
    @Nested
    class EngineTransformSchemaTest{
        HazelcastTaskService hazelcastTaskService;
        ClientMongoOperator clientMongoOperator;
        @BeforeEach
        void init(){
            clientMongoOperator = mock(ClientMongoOperator.class);
            hazelcastTaskService = new HazelcastTaskService(clientMongoOperator);
        }

        @DisplayName("test initializeModel main process")
        @Test
        void test1() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setSyncType("sync");
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)){
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
                when(clientMongoOperator.findOne(any(Query.class),any(),any())).thenReturn(transformerWsMessageDto);
                DAG cloneDag = mock(DAG.class);

                when(dag.clone()).thenReturn(cloneDag);
                when(cloneDag.transformSchema(any(),any(),any())).thenReturn(new HashMap<>());
                Map<String,TapTableMap<String, TapTable>> result = hazelcastTaskService.engineTransformSchema(taskDto);
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
            try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)){
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
                when(clientMongoOperator.findOne(any(Query.class),any(),any())).thenReturn(transformerWsMessageDto);
                DAG cloneDag = mock(DAG.class);
                when(dag.clone()).thenReturn(cloneDag);
                doThrow(new MongoClientException("")).when(cloneDag).transformSchema(any(),any(),any(),any());
                Assertions.assertThrows(TapCodeException.class,()->{
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
            try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<AspectUtils> mockedStatic = mockStatic(AspectUtils.class)){
                mockedStatic.when(() -> AspectUtils.executeAspect(any(), any())).thenReturn(null);
                obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Assertions.assertThrows(TapCodeException.class,()->{
                    hazelcastTaskService.engineTransformSchema(taskDto);
                });
            }

        }
    }

    @Nested
    class GetTapTableMapTest{
        @Test
        void testTaskTypeIsTestRun(){
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("testRun");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            try(MockedStatic<TapTableUtil> tapTableUtilMockedStatic = mockStatic(TapTableUtil.class)){
                tapTableUtilMockedStatic.when(()->TapTableUtil.getTapTableMapByNodeId(anyString(),any())).thenAnswer(invocationOnMock -> {
                    Assertions.assertEquals("databaseNode",invocationOnMock.getArgument(0));
                    return null;
                });
                HazelcastTaskService.getTapTableMap(taskDto,1L,databaseNode,new HashMap<>());
            }
        }

        @Test
        void testNormalTask(){
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            try(MockedStatic<TapTableMap> tableMapMockedStatic = mockStatic(TapTableMap.class)){
                tableMapMockedStatic.when(()->TapTableMap.create(anyString())).thenAnswer(invocationOnMock -> {
                    Assertions.assertEquals("databaseNode",invocationOnMock.getArgument(0));
                    return null;
                });
                HazelcastTaskService.getTapTableMap(taskDto,1L,databaseNode,new HashMap<>());
            }
        }

        @Test
        void testNormalTaskTapTableMapHashMapIsNotNull(){
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("databaseNode");
            Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
            TapTableMap<String, TapTable> except =  TapTableMap.create("databaseNode");
            tapTableMapHashMap.put("databaseNode",except);
            TapTableMap<String, TapTable> result = HazelcastTaskService.getTapTableMap(taskDto,1L,databaseNode,tapTableMapHashMap);
            Assertions.assertEquals(except,result);
        }
    }
}
