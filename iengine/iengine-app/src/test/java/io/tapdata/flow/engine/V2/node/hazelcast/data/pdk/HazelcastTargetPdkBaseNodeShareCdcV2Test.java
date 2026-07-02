package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.Setting;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.enums.SettingType;
import io.tapdata.flow.engine.V2.common.StreamReadTag;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.schema.TapTableMap;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("HazelcastTargetPdkBaseNode Share CDC Test")
class HazelcastTargetPdkBaseNodeShareCdcV2Test extends BaseHazelcastNodeTest {

    private HazelcastTargetPdkBaseNode hazelcastTargetPdkBaseNode;
    private DataProcessorContext dataProcessorContext;
    private ProcessorBaseContext processorBaseContext;
    private TaskDto taskDto;
    private TableNode tableNode;

    @BeforeEach
    void setUp() {
        reset(mockSettingService, mockClientMongoOperator, mockObsLogger);
        hazelcastTargetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class, Mockito.CALLS_REAL_METHODS);

        taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName("share-cdc-task");
        taskDto.setShareCdcEnable(true);
        taskDto.setType(TaskDto.TYPE_CDC);

        tableNode = new TableNode();
        tableNode.setId("target-node-id");
        tableNode.setName("target-node");
        tableNode.setConnectionId("connection-id");
        tableNode.setTableName("source_table");

        dataProcessorContext = mock(DataProcessorContext.class);
        when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
        when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);

        processorBaseContext = mock(ProcessorBaseContext.class);

        setBaseProperty(hazelcastTargetPdkBaseNode);
        ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
        ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "processorBaseContext", processorBaseContext);
        doReturn((Node) tableNode).when(hazelcastTargetPdkBaseNode).getNode();
    }

    @Nested
    @DisplayName("targetTableName method test")
    class TargetTableNameTest {
        @Test
        @DisplayName("sourceTableNames 为空时返回空列表")
        void testReturnEmptyListWhenSourceTableNamesIsEmpty() {
            List<String> actual = hazelcastTargetPdkBaseNode.targetTableName(Collections.emptyList());

            assertNotNull(actual);
            assertTrue(actual.isEmpty());
        }

        @Test
        @DisplayName("根据 ancestorsName 映射目标表并自动去重")
        void testMapAncestorsNameToTargetTableName() {
            Map<String, String> tableNameAndQualifiedNameMap = new HashMap<>();
            tableNameAndQualifiedNameMap.put("target_a", "qualified_target_a");
            tableNameAndQualifiedNameMap.put("target_b", "qualified_target_b");
            TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("nodeId", tableNameAndQualifiedNameMap);
            TapTable tableA = new TapTable("target_a");
            tableA.setAncestorsName("source_a");
            TapTable tableB = new TapTable("target_b");
            tableB.setAncestorsName("source_b");
            tapTableMap.put("target_a", tableA);
            tapTableMap.put("target_b", tableB);
            when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);

            List<String> actual = hazelcastTargetPdkBaseNode.targetTableName(Arrays.asList("source_a", "source_a", "missing"));

            assertEquals(Set.of("target_a"), new HashSet<>(actual));
        }
    }

    @Nested
    @DisplayName("initShareCdcCollectorIfNeed method test")
    class InitShareCdcCollectorIfNeedTest {
        @Test
        @DisplayName("任务未开启共享挖掘时直接返回")
        void testReturnWhenTaskShareCdcDisabled() {
            taskDto.setShareCdcEnable(false);

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
            verifyNoInteractions(mockSettingService, mockClientMongoOperator);
        }

        @Test
        @DisplayName("系统未配置共享挖掘开关时直接返回")
        void testReturnWhenGlobalSettingIsMissing() {
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(null);

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
            verify(mockSettingService).loadSettings(SettingType.SHARE_CDC_ENABLE.getKey());
            verify(mockClientMongoOperator, never()).postOne(anyMap(), anyString(), eq(Map.class));
        }

        @Test
        @DisplayName("任务未开启 CDC 时不初始化共享挖掘")
        void testReturnWhenTaskNotOpenCdc() {
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC);
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(enabledShareCdcSetting());

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
            verify(mockClientMongoOperator, never()).postOne(anyMap(), anyString(), eq(Map.class));
        }

        @Test
        @DisplayName("系统关闭共享挖掘时直接返回")
        void testReturnWhenGlobalShareCdcDisabled() {
            Setting setting = new Setting();
            setting.setValue("false");
            setting.setDefault_value("false");
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(setting);

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
            verify(mockClientMongoOperator, never()).postOne(anyMap(), anyString(), eq(Map.class));
        }

        @Test
        @DisplayName("CDC 表节点初始化后会立即注册共享挖掘表")
        void testInitAndStartShareCdcForCdcTableNode() {
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(enabledShareCdcSetting());
            when(processorBaseContext.getTapTableMap()).thenReturn(targetTableMap());

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
            verify(mockClientMongoOperator).postOne(bodyCaptor.capture(), eq("logcollector/start-and-wait"), eq(Map.class));
            Map<String, Object> body = bodyCaptor.getValue();
            assertEquals("connection-id", body.get("connectionId"));
            assertEquals(List.of("source_table"), body.get("tableNames"));
            assertEquals(false, ((Map<?, ?>) body.get("nodeConfig")).get("enableFillingModifiedData"));
            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
        }

        @Test
        @DisplayName("CDC 数据库节点初始化后会回填表名并注册共享挖掘表")
        void testInitAndStartShareCdcForCdcDatabaseNode() {
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("database-node-id");
            databaseNode.setName("database-node");
            databaseNode.setConnectionId("connection-id");
            when(dataProcessorContext.getNode()).thenReturn((Node) databaseNode);
            doReturn((Node) databaseNode).when(hazelcastTargetPdkBaseNode).getNode();
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(enabledShareCdcSetting());
            when(processorBaseContext.getTapTableMap()).thenReturn(targetTableMap());

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            ArgumentCaptor<Map<String, Object>> bodyCaptor = ArgumentCaptor.forClass(Map.class);
            verify(mockClientMongoOperator).postOne(bodyCaptor.capture(), eq("logcollector/start-and-wait"), eq(Map.class));
            assertEquals(List.of("target_table"), bodyCaptor.getValue().get("tableNames"));
            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
        }

        @Test
        @DisplayName("非 CDC 任务仅初始化 streamReadTag 并忽略空表集合")
        void testKeepStreamReadTagForNonCdcTaskAndIgnoreEmptyTableNames() {
            taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(enabledShareCdcSetting());
            when(processorBaseContext.getTapTableMap()).thenReturn(targetTableMap());

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            StreamReadTag streamReadTag = (StreamReadTag) ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag");
            assertNotNull(streamReadTag);
            streamReadTag.accept(Collections.emptyList());
            verify(mockClientMongoOperator, never()).postOne(anyMap(), anyString(), eq(Map.class));
        }

        @Test
        @DisplayName("注册共享挖掘表异常时应被捕获")
        void testIgnoreExceptionWhenStartShareCdcFailed() {
            when(mockSettingService.getSetting(SettingType.SHARE_CDC_ENABLE.getKey())).thenReturn(enabledShareCdcSetting());
            when(processorBaseContext.getTapTableMap()).thenReturn(targetTableMap());
            when(mockClientMongoOperator.postOne(anyMap(), anyString(), eq(Map.class))).thenThrow(new RuntimeException("share cdc failed"));

            hazelcastTargetPdkBaseNode.initShareCdcCollectorIfNeed();

            verify(mockClientMongoOperator).postOne(anyMap(), anyString(), eq(Map.class));
            assertNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "streamReadTag"));
        }

        private Setting enabledShareCdcSetting() {
            Setting setting = new Setting();
            setting.setValue("true");
            setting.setDefault_value("false");
            return setting;
        }

        private TapTableMap<String, TapTable> targetTableMap() {
            Map<String, String> tableNameAndQualifiedNameMap = new HashMap<>();
            tableNameAndQualifiedNameMap.put("target_table", "qualified_target_table");
            TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("target-node-id", tableNameAndQualifiedNameMap);
            TapTable tapTable = new TapTable("target_table");
            tapTable.setAncestorsName("source_table");
            tapTableMap.put("target_table", tapTable);
            return tapTableMap;
        }
    }
}
