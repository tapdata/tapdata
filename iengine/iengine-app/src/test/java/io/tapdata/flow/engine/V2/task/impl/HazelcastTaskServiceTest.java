package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastBlank;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastVirtualTargetNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.*;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

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
        @DisplayName("test createNode method for hazelcastIMDG")
        void testCreateNode16(){
            when(node.getType()).thenReturn("hazelcastIMDG");
            when(taskDto.getType()).thenReturn("initial_sync");
            HazelcastBaseNode actual = HazelcastTaskService.createNode(taskDto, nodes, edges, node, predecessors, successors, config, connection, databaseType, mergeTableMap, tapTableMap, taskConfig);
            assertEquals(HazelcastTargetPdkShareCDCNode.class, actual.getClass());
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
    }
}
